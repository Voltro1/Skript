package org.skriptlang.skript.variables.storage.flatfile;

import ch.njol.skript.Skript;
import ch.njol.skript.lang.Variable;
import ch.njol.skript.util.ExceptionUtils;
import ch.njol.skript.variables.SerializedVariable;
import org.jetbrains.annotations.Blocking;
import org.jetbrains.annotations.NonBlocking;
import org.jetbrains.annotations.Nullable;
import org.jspecify.annotations.NonNull;
import org.skriptlang.skript.variables.CsvFormat;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.IntSupplier;

/**
 * Single writer for a CSV variable file.
 * <p>
 * The file is an append-only log, occasionally compacted. Every change is appended as a CSV row,
 * the last row for a name wins, and a row of type {@code null} deletes the name.
 * When superseded rows pile up, the file is rewritten from {@link #persisted}.
 * <p>
 * Only {@link #submit}, {@link #flush} and {@link #stop} may be called from other threads.
 * Everything else runs on the writer thread, which owns the file and {@link #persisted}.
 * The writer never serializes values and never waits on the main thread.
 */
final class CsvVariableWriter {


	/**
	 * Backup hooks provided by the owning storage.
	 */
	interface Backups {
		/**
		 * Records that the persisted state changed since the last backup.
		 */
		void changed();

		/**
		 * Whether a backup should be taken now.
		 */
		boolean due();

		/**
		 * Takes a backup of the variable file if one is due.
		 */
		void backup() throws IOException;
	}

	/**
	 * An operation to perform on the writer thread.
	 */
	private sealed interface Operation permits Change, Barrier {
	}

	/**
	 * An ordered change. Null values indicate a deletion.
	 * If {@code isList} is true, the value is a list of values, otherwise it is a single value.
	 * Lists will never be set to a value, they can only be cleared by setting them to null.
	 */
	private record Change(String name, boolean isList, SerializedVariable.@Nullable Value value) implements Operation {
	}

	/**
	 * Completed once every earlier change is flushed and synced.
	 * Used to ensure that the writer thread has finished processing all changes before stopping or flushing.
	 */
	private record Barrier(CompletableFuture<Boolean> done) implements Operation {
	}

	/**
	 * The outcome of {@link #stop}.
	 */
	enum StopResult {
		/**
		 * The writer exited after writing and syncing every change.
		 */
		SAVED,
		/**
		 * The writer exited, but gave up before every change was saved.
		 */
		FAILED,
		/**
		 * The writer did not exit in time and may still be using the file.
		 */
		TIMED_OUT
	}

	// ---- Constants ----

	/**
	 * How long to wait after a fresh change to allow the queue to fill up and take advantage
	 * of repeat changes merging.
	 */
	private static final long LINGER_NANOS = TimeUnit.MILLISECONDS.toNanos(50);


	/**
	 * How long to wait idle before waking up to check if backups are needed
	 */
	private static final long IDLE_CHECK_NANOS = TimeUnit.SECONDS.toNanos(5);

	/**
	 * How long to wait (at minimum) between rewrites, to avoid spam-rewriting smaller variable files.
	 */
	private static final long MIN_REWRITE_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);

	/**
	 * How long (at minimum) between fsyncs (forcing the data to write to disk). Longer values mean more data at risk during a
	 * hard crash, but also less disk work for the server.
	 */
	private static final long FSYNC_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(1);

	/**
	 * How many changes can be processed at once before the writer stops this batch.
	 * Prevents getting stalled when many changes are coming in at once and gives
	 * the writer a chance to fsync, merge, and compact.
	 */
	private static final int MAX_BATCH = 10_000;

	/**
	 * If superseded lines exceed live lines by this factor, then force a rewrite.
	 */
	private static final int HARD_REWRITE_FACTOR = 10;

	/**
	 * How long to wait between attempting a new backup if the previous one failed
	 */
	private static final long BACKUP_RETRY_NANOS = TimeUnit.MINUTES.toNanos(5);

	/**
	 * How many times a stop() can fail before we give up.
	 * Each attempt will wait 1 second before trying again.
	 */
	private static final int MAX_FAILURES_WHILE_STOPPING = 3;

	// ---- Shared ----

	private final Queue<Operation> queue = new ConcurrentLinkedQueue<>(); // changes and barriers submitted from other threads
	private final Thread thread; // the writer thread, which owns the file and persisted state
	private final AtomicBoolean sleeping = new AtomicBoolean(); // whether the writer thread is currently sleeping
	private volatile boolean skipDelays; // used to skip the usual linger duration. For flushes.
	private volatile boolean stopping; // set by stop() to tell the writer thread to exit
	private volatile boolean saved; // set by the writer thread once its final save succeeded

	// ---- Configuration ----

	private final Path file; // the variable file to write to
	private final String databaseName; // the configured database name, for messages
	private final IntSupplier requiredChangesForResave; // minimum number of superseded rows before a rewrite
	private final Backups backups; // backup hooks of the owning storage, so the writer can trigger backups when it changes the file

	// ---- Writer-thread state ----

	// the last persisted value of each variable, owned by the writer thread
	// this is somewhat significant overhead, but I think it's worth it to have an accurate record to rewrite the file
	// from if anything goes wrong.
	private final TreeMap<String, SerializedVariable.Value> persisted;

	private long rowsInFile; // number of variable rows currently in the file, including superseded ones
	private boolean rewriteRequired; // whether the file must be rewritten before anything is appended
	private long lastRewrite = System.nanoTime() - MIN_REWRITE_INTERVAL_NANOS;
	private @Nullable FileChannel channel; // the file channel for the variable file, opened on the writer thread
	private @Nullable PrintWriter out; // the print writer for the variable file, opened on the writer thread
	private long lastSync = System.nanoTime(); // last time the file was synced to disk, in nanoseconds
	private boolean unsynced; // whether rows have been flushed to the file but not yet synced to disk

	/**
	 * Creates a new writer for the given variable file.
	 *
	 * @param file                     the variable file
	 * @param databaseName             the configured database name, for messages
	 * @param persisted                A map for the writer to track the last persisted value of each variable.
	 *                                 The writer owns this map and may modify it.
	 * @param rowsInFile               number of variable rows currently in the file, including superseded ones
	 * @param rewriteRequired          whether the file must be rewritten before anything is appended
	 * @param requiredChangesForResave minimum number of superseded rows before a rewrite
	 * @param backups                  backup hooks of the owning storage, so the writer can trigger backups when it changes the file
	 */
	CsvVariableWriter(Path file, String databaseName, Map<String, SerializedVariable.Value> persisted,
	                  long rowsInFile, boolean rewriteRequired, IntSupplier requiredChangesForResave, Backups backups) {
		this.file = file;
		this.databaseName = databaseName;
		this.persisted = new TreeMap<>(persisted);
		this.rowsInFile = rowsInFile;
		this.rewriteRequired = rewriteRequired;
		this.requiredChangesForResave = requiredChangesForResave;
		this.backups = backups;
		this.thread = Skript.newThread(this::run, "Skript variable writer for database '" + databaseName + "'");
		this.thread.setDaemon(true); // stop() joins it, so a dead disk must not keep the JVM alive
	}

	/**
	 * Opens the file for appending and starts the writer thread.
	 *
	 * @throws IOException if the file cannot be opened
	 */
	void start() throws IOException {
		openAppend();
		thread.start();
	}

	// ---- Called from other threads ----

	/**
	 * Queues a change. Changes are written in submission order, so callers must submit
	 * changes to the same names in the same order they applied them in memory.
	 *
	 * @param name  variable name, or a list name ending in {@code ::*}
	 * @param value serialized value, or null to delete the variable or clear the list. (If {@code name} ends in {@code ::*}, {@code value} must be null.)
	 */
	@NonBlocking
	void submit(@NonNull String name, SerializedVariable.@Nullable Value value) {
		boolean list = name.endsWith(Variable.SEPARATOR + "*");
		assert !list || value == null : name;
		queue.offer(new Change(name, list, value));
		// Wake only idle writers, and only once per sleep. We don't want to be trying to wake it constantly.
		if (sleeping.get() && sleeping.compareAndSet(true, false))
			LockSupport.unpark(thread);
	}

	/**
	 * Flushes all the pending changes. Blocks until the writer thread has written and synced them, or the timeout expires.
	 *
	 * @param timeout maximum time to wait
	 * @param unit    unit of {@code timeout}
	 * @return whether the changes are durable
	 */
	@Blocking
	boolean flush(long timeout, TimeUnit unit) {
		if (stopping || !thread.isAlive())
			return false;
		Barrier barrier = new Barrier(new CompletableFuture<>());
		queue.offer(barrier);
		skipDelays = true; // don't want to wait around for 50ms!!!
		LockSupport.unpark(thread);
		try {
			return barrier.done().get(timeout, unit);
		} catch (InterruptedException exception) {
			Thread.currentThread().interrupt();
			return false;
		} catch (ExecutionException | TimeoutException exception) {
			return false;
		}
	}

	/**
	 * Writes all remaining changes, compacts the file and stops the writer thread.
	 * Blocks until the writer thread has exited, or the timeout expires.
	 * Callers must stop submitting changes before calling this.
	 *
	 * @param timeout maximum time to wait
	 * @param unit    unit of {@code timeout}
	 * @return whether the writer saved everything, gave up, or is still running
	 */
	@Blocking
	StopResult stop(long timeout, @NonNull TimeUnit unit) {
		stopping = true;
		LockSupport.unpark(thread);
		boolean interrupted = false;
		try {
			thread.join(unit.toMillis(timeout));
		} catch (InterruptedException exception) {
			interrupted = true;
		}
		if (interrupted)
			Thread.currentThread().interrupt();
		if (thread.isAlive())
			return StopResult.TIMED_OUT;
		return saved ? StopResult.SAVED : StopResult.FAILED;
	}

	// ---- Writer thread methods ----

	/**
	 * The main loop for the writer.
	 * It will spin in this loop for its entire lifetime, parking itself between iterations.
	 */
	private void run() {
		int failuresWhileStopping = 0;
		long backupRetryAt = System.nanoTime();
		boolean backlog = false; // if we have queued changes from last iteration
		while (true) {
			boolean stop = stopping; // read before draining so nothing submitted before stop() is missed
			try {
				// check whether we should compact
				if (rewriteRequired || shouldCompact())
					rewrite();
				// apply pending changes. If our queue is currently empty, linger around in case more changes are coming in.
				// Don't want to spam apply a handful of changes at a time.
				if (!queue.isEmpty()) {
					if (!backlog)
						linger();
					drain();
					backlog = !queue.isEmpty();
				}
				// sync rows left over from a throttled sync, once the interval has passed
				if (unsynced)
					flushOut(false);

				// check if we need to do a backup
				if (System.nanoTime() - backupRetryAt >= 0 && backups.due()) {
					if (rowsInFile != persisted.size())
						rewrite(); // back up a clean, complete snapshot
					try {
						backups.backup();
					} catch (IOException exception) {
						Skript.error("Cannot back up database '" + databaseName + "': " + ExceptionUtils.toString(exception)
							+ ". Retrying in " + TimeUnit.NANOSECONDS.toMinutes(BACKUP_RETRY_NANOS) + " minutes.");
						backupRetryAt = System.nanoTime() + BACKUP_RETRY_NANOS;
					}
				}

				// If we're stopping and have processed the queue, do a final compact before exiting.
				if (stop && queue.isEmpty()) {
					if (rowsInFile != persisted.size())
						rewrite();
					saved = true;
					break;
				}
			} catch (IOException | RuntimeException exception) {
				// `persisted` holds every applied change, so a rewrite repairs any partial append.
				rewriteRequired = true;
				backlog = false;
				if (stop && ++failuresWhileStopping >= MAX_FAILURES_WHILE_STOPPING) {
					reportGivingUp(exception);
					break;
				}
				reportWriteFailure(exception);
				LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(stop ? 1 : 5));
				continue;
			}
			// wait for next time
			if (!backlog && !stop)
				idle();
		}
		// we're closing, so invalidate any waiting barriers and close the file
		failPendingBarriers();
		closeAppend();
	}

	/**
	 * I/O failures are environmental (disk full, permissions), so they get a one-line error;
	 * anything else is a bug and gets a full exception report.
	 */
	private void reportWriteFailure(Exception exception) {
		String message = "Failed writing variables for database '" + databaseName + "'; retrying";
		if (exception instanceof IOException ioException)
			Skript.error(message + ": " + ExceptionUtils.toString(ioException));
		else
			Skript.exception(exception, message);
	}

	/**
	 * Reports that the writer is stopping without saving everything.
	 * The file failed to update, so it may be missing changes that were already applied, on top of any still queued.
	 */
	private void reportGivingUp(Exception exception) {
		long queued = queue.stream().filter(Change.class::isInstance).count();
		String message = "Giving up saving database '" + databaseName + "' after " + MAX_FAILURES_WHILE_STOPPING
			+ " attempts. " + queued + " queued changes were not saved, and the variable file may be missing recent changes";
		if (exception instanceof IOException ioException)
			Skript.error(message + ": " + ExceptionUtils.toString(ioException));
		else
			Skript.exception(exception, message);
	}


	/**
	 * Waits around a little bit to allow the queue to fill up a bit before writing.
	 * This is a trade-off in that we can lose up to {@link #LINGER_NANOS} of data on a
	 * hard crash, but in exchange we skip writing many duplicate lines to file when
	 * a single variable is changing a lot.
	 * If we need to process NOW, set {@link #skipDelays} to skip the wait.
	 */
	private void linger() {
		long deadline = System.nanoTime() + LINGER_NANOS;
		while (!stopping && !skipDelays) {
			long remaining = deadline - System.nanoTime();
			if (remaining <= 0)
				break;
			LockSupport.parkNanos(remaining);
		}
		skipDelays = false;
	}

	/**
	 * Parks the writer for a while, waiting on a submit/flush/stop.
	 * If nothing occurs before {@link #IDLE_CHECK_NANOS}, the writer will wake up to check if it needs to do backups.
	 * If there are unsynced rows, it wakes up in time to sync them once {@link #FSYNC_INTERVAL_NANOS} has passed.
	 */
	private void idle() {
		long wait = unsynced ? lastSync + FSYNC_INTERVAL_NANOS - System.nanoTime() : IDLE_CHECK_NANOS;
		sleeping.set(true);
		// Re-check after publishing the flag, so a submit that missed the flag is not lost.
		if (queue.isEmpty() && !stopping && !skipDelays && wait > 0)
			LockSupport.parkNanos(wait);
		sleeping.set(false);
	}

	/**
	 * Applies up to one batch of queued changes and appends their rows.
	 * Repeated changes to the same name are merged to avoid repeat writes,
	 * but barriers prevent merges across them to maintain cohesion with things like list clears.
	 */
	private void drain() throws IOException {
		// just make sure we can actually write
		if (out == null)
			openAppend();

		LinkedHashMap<String, SerializedVariable.@Nullable Value> merged = new LinkedHashMap<>();
		boolean wrote = false;
		Operation op;
		int drained = 0;
		// for each pending change, limited to MAX_BATCH at once
		while (drained++ < MAX_BATCH && (op = queue.poll()) != null) {
			if (op instanceof Change(String name, boolean isList, SerializedVariable.Value value) && !isList) {
				// apply change
				merged.remove(name); // keep each name at the position of its last change
				merged.put(name, value);
				continue;
			}
			// We can't merge across barriers/list clears, so write what we have so far before moving on.
			wrote |= writeMerged(merged);
			if (op instanceof Change clear) {
				// list clear
				apply(clear);
				wrote = true;
			} else if (op instanceof Barrier(CompletableFuture<Boolean> done)) {
				// write what we have so far to file
				try {
					flushOut(true);
					wrote = false;
					done.complete(true);
				} catch (IOException exception) {
					done.complete(false);
					throw exception;
				}
			}
		}
		// Write all our changes to file
		wrote |= writeMerged(merged);
		if (wrote)
			flushOut(false);
	}

	/**
	 * Simple helper to apply merged changes.
	 *
	 * @param merged A map of all changes applied so far, in order but merged to just the most recent change for each var
	 * @return Whether anything was applied
	 */
	private boolean writeMerged(@NonNull LinkedHashMap<String, SerializedVariable.@Nullable Value> merged) {
		if (merged.isEmpty())
			return false;
		merged.forEach((name, value) -> apply(new Change(name, false, value)));
		merged.clear();
		return true;
	}

	/**
	 * Applies a change to {@link #persisted} and appends a row to the file.
	 *
	 * @param change The change to apply
	 */
	private void apply(Change change) {
		if (change.isList()) {
			// list removal
			// clear out list elements in persisted
			String prefix = CsvFormat.listPrefix(change.name());
			persisted.subMap(prefix, CsvFormat.prefixEnd(prefix)).clear();
			// write removal to file
			appendRow(change.name(), null);
		} else if (change.value() == null) {
			// normal removal
			if (persisted.remove(change.name()) != null)
				appendRow(change.name(), null);
		} else {
			// normal change
			persisted.put(change.name(), change.value());
			appendRow(change.name(), change.value());
		}
		// tell backups hook we're actually different now
		backups.changed();
	}

	/**
	 * Simple helper to write a var to a csv row.
	 *
	 * @param name  The variable name
	 * @param value The variable value
	 */
	private void appendRow(String name, SerializedVariable.@Nullable Value value) {
		assert out != null;
		CsvFormat.writeCSV(out, name,
			value == null ? "null" : value.type(),
			value == null ? "" : CsvFormat.encode(value.data()));
		rowsInFile++;
	}

	/**
	 * Forces the out stream to flush to the file. fsync only if it's been more than {@link #FSYNC_INTERVAL_NANOS} or
	 * {@code sync} is set.
	 *
	 * @param sync Whether to force an fsync.
	 */
	private void flushOut(boolean sync) throws IOException {
		PrintWriter out = this.out;
		FileChannel channel = this.channel;
		if (out == null || channel == null)
			throw new IOException("Variable file is not open");
		out.flush();
		if (out.checkError())
			throw new IOException("Failed appending to the variable file");
		long now = System.nanoTime();
		if (sync || now - lastSync >= FSYNC_INTERVAL_NANOS) {
			channel.force(false);
			lastSync = now;
			unsynced = false;
		} else {
			unsynced = true;
		}
	}

	/**
	 * Helper to check if we've written enough superseded lines to make a rewrite worthwhile.
	 * True if:
	 * <ul>
	 *     <li>Superseded lines exceed {@link #requiredChangesForResave}</li>
	 *     <li>AND It has been more than {@link #MIN_REWRITE_INTERVAL_NANOS} since the last rewrite</li>
	 *     <li>OR superseded lines exceed {@code HARD_REWRITE_FACTOR * live + requiredChangesForResave}</li>
	 * </ul>
	 *
	 * @return Whether we should compact the var file.
	 */
	private boolean shouldCompact() {
		long live = persisted.size();
		long superseded = rowsInFile - live;
		if (superseded < Math.max(requiredChangesForResave.getAsInt(), live))
			return false;
		if (System.nanoTime() - lastRewrite >= MIN_REWRITE_INTERVAL_NANOS)
			return true;
		return superseded >= HARD_REWRITE_FACTOR * live + requiredChangesForResave.getAsInt();
	}

	/**
	 * Rewrites the variables file based on {@link #persisted}, removing all the superseded lines.
	 */
	private void rewrite() throws IOException {
		Path temp = file.resolveSibling(file.getFileName() + ".temp");

		// write every persisted var to temp file.
		try (FileChannel tempChannel = FileChannel.open(temp, StandardOpenOption.CREATE,
			StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
		     PrintWriter writer = new PrintWriter(new BufferedWriter(
				 Channels.newWriter(tempChannel, CsvFormat.CHARSET)))) {
			CsvFormat.writeHeader(writer);
			persisted.forEach((name, value) -> CsvFormat.writeRow(writer, name, value));
			writer.flush();
			if (writer.checkError())
				throw new IOException("Failed writing " + temp.getFileName());
			tempChannel.force(true);
		}
		// write succeeded, so close and replace existing with temp
		closeAppend();
		Files.move(temp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
		rowsInFile = persisted.size();
		rewriteRequired = false;
		lastRewrite = System.nanoTime();
		openAppend();
	}

	/**
	 * Opens the variables file for appending
	 */
	private void openAppend() throws IOException {
		FileChannel channel = FileChannel.open(file, StandardOpenOption.CREATE,
			StandardOpenOption.WRITE, StandardOpenOption.APPEND);
		this.channel = channel;
		this.out = new PrintWriter(new BufferedWriter(Channels.newWriter(channel, CsvFormat.CHARSET)));
		this.lastSync = System.nanoTime();
	}

	/**
	 * Closes the variable file, flushing the printwriter with fsync.
	 */
	private void closeAppend() {
		PrintWriter out = this.out;
		FileChannel channel = this.channel;
		this.out = null;
		this.channel = null;
		unsynced = false; // synced below, and flushOut must not run on a closed file
		if (out == null || channel == null)
			return;
		out.flush();
		try {
			channel.force(false);
		} catch (IOException exception) {
			Skript.exception(exception, "Cannot sync the variable file of database '" + databaseName + "'");
		}
		out.close();
	}

	/**
	 * Simple helper to cause all pending barriers to fail, used when stopping.
	 */
	private void failPendingBarriers() {
		for (Operation op : new ArrayList<>(queue)) {
			if (op instanceof Barrier(CompletableFuture<Boolean> done))
				done.complete(false);
		}
	}

}
