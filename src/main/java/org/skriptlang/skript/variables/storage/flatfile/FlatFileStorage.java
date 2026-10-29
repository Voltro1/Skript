package org.skriptlang.skript.variables.storage.flatfile;

import ch.njol.skript.Skript;
import ch.njol.skript.config.SectionNode;
import ch.njol.skript.lang.Variable;
import ch.njol.skript.log.BlockingLogHandler;
import ch.njol.skript.log.SkriptLogger;
import ch.njol.skript.registrations.Classes;
import ch.njol.skript.util.FileUtils;
import ch.njol.skript.util.Version;
import ch.njol.skript.variables.SerializedVariable;
import ch.njol.skript.variables.SerializedVariable.Value;
import ch.njol.skript.variables.VariableStorage;
import ch.njol.skript.variables.Variables;
import ch.njol.skript.variables.VariablesMap;
import com.google.common.util.concurrent.Striped;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;
import org.jspecify.annotations.NonNull;
import org.skriptlang.skript.addon.SkriptAddon;
import org.skriptlang.skript.variables.CsvFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

/**
 * A variable storage that stores its content in a
 * comma-separated value file (CSV file).
 */
public class FlatFileStorage extends VariableStorage {

	/**
	 * The {@link Charset} used in the CSV storage file.
	 */
	public static final Charset FILE_CHARSET = CsvFormat.CHARSET;

	/**
	 * Minimum number of superseded rows before the file is rewritten, from the global
	 * {@code variable changes until save} option. Used by databases that don't set
	 * {@code required changes per save} themselves, and read live so a config reload applies.
	 */
	public static volatile int defaultRequiredChangesForResave = 1000;

	/**
	 * The timeout for flushing changes to disk. If the flush takes longer than this, it will be aborted and return false.
	 */
	private static final long FLUSH_TIMEOUT_SECONDS = 30;

	/**
	 * The timeout for closing the storage. If the close takes longer than this, it will be aborted and return false.
	 */
	private static final long CLOSE_TIMEOUT_SECONDS = 60;

	private final AtomicBoolean closed = new AtomicBoolean();
	private final AtomicBoolean warnedClosed = new AtomicBoolean();

	// failure logging
	private final Set<String> reportedFailures = ConcurrentHashMap.newKeySet();
	private final AtomicBoolean failuresSuppressed = new AtomicBoolean();
	private static final int MAX_REPORTED_FAILURES = 50;

	/**
	 * A striped lock that is used to synchronize variables sets, to ensure the writer doesn't get out of sync
	 * with the variables map. The lock is striped to reduce contention, so changes to variables with differing prefixes
	 * don't contend with each other.
	 * {@link #getLockFor(String)} is used to get the appropriate lock for a variable name.
	 */
	private final Striped<Lock> setLocks = Striped.lock(64);

	/**
	 * The variables map that stores the variables in memory.
	 * Writes to this map should be synchronized with the {@link #setLocks} to ensure consistency between the map and the writer.
	 */
	private final VariablesMap variables = new VariablesMap();

	/**
	 * The writer that handles writing the changes to disk. This writer runs in a separate thread and wakes up
	 * when there are changes to write, or when the storage is closed. It is volatile to ensure visibility across threads.
	 */
	private volatile @Nullable CsvVariableWriter writer;

	/**
	 * An interface that represents the result of serializing a variable value. It can be one of three types:
	 * <ul>
	 *     <li>{@link Bytes}: the variable value was successfully serialized to bytes and should be persisted to disk.</li>
	 *     <li>{@link Delete}: the variable value should be deleted from disk, either because it was removed or because it is a list variable that was cleared.</li>
	 *     <li>{@link Failed}: the variable value could not be serialized and should be kept in memory only, leaving the old value on disk.</li>
	 * </ul>
	 * This interface is used to communicate the result of serializing a variable value to the code
	 * that handles writing the changes to disk, so that it can take the appropriate action based on the result.
	 */
	private sealed interface Serialized {
		/**
		 * Persist these bytes.
		 *
		 * @param value the serialized value to persist
		 */
		record Bytes(Value value) implements Serialized {
		}

		/**
		 * Persist as a delete: deletion, list clear, or a type with no serializer.
		 */
		record Delete() implements Serialized {
		}

		/**
		 * Serialization failed: keep the value in memory only and leave the old row on disk.
		 */
		record Failed(@Nullable Throwable cause) implements Serialized {
		}
	}

	/**
	 * Create a new CSV storage of the given name.
	 *
	 * @param source the source of this storage.
	 * @param type   the database type i.e. CSV.
	 */
	public FlatFileStorage(SkriptAddon source, String type) {
		super(source, type);
	}

	@Override
	protected boolean load(SectionNode sectionNode) {
		SkriptLogger.setNode(null);

		// sanity check
		File file = this.file;
		if (file == null) {
			assert false : this;
			return false;
		}

		// Rows are applied in order. The last row for a name wins, and a list clear row removes
		// everything under its prefix that was written before it. Sorted, so prefix clears are range removals.
		TreeMap<String, SerializedVariable.Value> rows = new TreeMap<>();
		LoadReport report = new LoadReport();
		boolean rewriteRequired = false;
		long rowCount = 0;

		// parse the file
		try (BufferedReader reader = Files.newBufferedReader(file.toPath(), CsvFormat.CHARSET)) {
			// Every row is written with a trailing newline, so an unterminated last row was torn by a crash mid-append.
			// It is treated as never written, since a torn value can still parse and would replace the previous value.
			boolean tornTail = !endsWithNewline(file.toPath());
			String line;
			String next = reader.readLine(); // read one line ahead to know which line is last
			int lineNum = 0;
			while ((line = next) != null) {
				next = reader.readLine();
				lineNum++;
				if (next == null && tornTail) {
					report.add("incomplete last rows (discarded)", "line " + lineNum);
					rewriteRequired = true; // never append onto a torn last line
					break;
				}
				line = line.trim();
				// version check
				if (line.startsWith(CsvFormat.VERSION_HEADER)) {
					try {
						boolean legacy = new Version(line.substring(CsvFormat.VERSION_HEADER.length()).trim()).isSmallerThan(new Version(2, 1));
						if (legacy) {
							Skript.error("Legacy variable files (<2.1) are no longer supported. No variables will be parsed.");
							return false;
						}
					} catch (IllegalArgumentException exception) {
						Skript.warning("Ignoring invalid version header in " + file.getName() + " at line " + lineNum);
					}
					continue;
				}
				// ignore comments
				if (line.isEmpty() || line.startsWith("#"))
					continue;
				rowCount++;
				// parse row
				String[] split = CsvFormat.splitCSV(line);
				if (split == null || split.length != 3) {
					report.add("invalid rows", "line " + lineNum);
					continue;
				}

				String name = split[0];
				boolean listName = name.endsWith(Variable.SEPARATOR + "*");

				// handle deletions
				if (split[1].equals(CsvFormat.DELETED)) {
					if (listName) {
						// lists have to delete any children we already read, in case the file wasn't compacted
						String prefix = CsvFormat.listPrefix(name);
						String end = CsvFormat.prefixEnd(prefix);
						rows.subMap(prefix, end).clear();
					} else {
						// normal delete
						rows.remove(name);
					}
					continue;
				}

				// sanity check
				if (listName) {
					report.add("list variables holding a value", name);
					continue;
				}

				// read values
				try {
					rows.put(name, new SerializedVariable.Value(split[1], CsvFormat.decode(split[2])));
				} catch (IllegalArgumentException exception) {
					report.add("unreadable values", name + " (line " + lineNum + ")");
				}
			}
		} catch (IOException exception) {
			Skript.exception(exception, "Cannot read the variable file " + file.getName());
			return false;
		}

		// Deserialized the values we read
		Set<SerializedVariable> encoded = new HashSet<>();
		rows.forEach((name, value) -> encoded.add(new SerializedVariable(name, value)));
		Map<String, Object> decoded;
		// Classes logs one error per failed variable; the summary below replaces those.
		// TODO: replace if errors removed from Classes.deserialize()
		try (BlockingLogHandler ignored = new BlockingLogHandler().start()) {
			decoded = Classes.deserialize(encoded);
		}
		if (decoded == null) {
			Skript.error("Cannot load " + file.getName() + ": some variables must be loaded on the main thread");
			return false;
		}
		for (String name : rows.keySet()) {
			// Undecodable rows stay in the persisted index, so rewrites keep them for a later fix.
			if (!decoded.containsKey(name))
				report.add("values that could not be deserialized (kept in the file)", name);
		}

		// Setup variables map and persisted map for the writer.
		Map<String, Value> persisted = new HashMap<>(rows);
		decoded.forEach(variables::setVariable);

		// make a backup if we found issues
		report.print(file.getName());
		if (!report.isEmpty()) {
			try {
				File backup = FileUtils.backup(file);
				Skript.info("Created a backup of " + file.getName() + " as " + backup.getName());
			} catch (IOException exception) {
				Skript.exception(exception, "Cannot back up " + file.getName() + " before rewriting it");
				return false;
			}
			// gonna have to re-write from the vars we could read
			rewriteRequired = true;
		}

		// setup writer
		CsvVariableWriter writer = new CsvVariableWriter(file.toPath(), getUserConfigurationName(), persisted,
			rowCount, rewriteRequired, () -> requiredChangesForResave, new CsvVariableWriter.Backups() {
			@Override
			public void changed() {
				markBackupChanged();
			}

			@Override
			public boolean due() {
				return backupsToKeep != 0 && isBackupDue();
			}

			@Override
			public void backup() throws IOException {
				backupIfDue();
			}
		});

		// start your engines!!!
		try {
			writer.start();
		} catch (IOException exception) {
			Skript.exception(exception, "Cannot open " + file.getName() + " for writing");
			return false;
		}
		this.writer = writer;
		return true;
	}

	@Override
	public long loadedVariables() {
		return variables.size();
	}

	@Override
	public Set<String> getStoredVariableNames() {
		return variables.getAll().keySet();
	}

	@Override
	public @Nullable Object getVariable(String name) {
		return variables.getVariable(name);
	}

	@Override
	public void setVariable(String name, @Nullable Object value) {
		// Ensure no one tries to set a list variables to a non-null value.
		// Lists can only be 'set' to null, which clears them.
		boolean isList = name.endsWith(Variable.SEPARATOR + "*");
		if (isList && value != null)
			throw new IllegalArgumentException("List variables can only be deleted: " + name);

		// Serialize the variable. May wait on the main thread for sync-only types.
		Serialized serialized = trySerialize(value);
		if (serialized instanceof Serialized.Failed(Throwable cause))
			reportFailure(name, cause);

		// Lock to ensure the change queue and variables map stay in sync.
		Lock lock = getLockFor(name);
		lock.lock();
		try {
			variables.setVariable(name, value);
			if (serialized instanceof Serialized.Failed)
				return; // memory only, the old row stays on disk
			CsvVariableWriter writer = this.writer;
			if (closed.get() || writer == null) { // warn if the storage is closed
				if (warnedClosed.compareAndSet(false, true))
					Skript.warning("Variables changed after database '" + getUserConfigurationName()
						+ "' was closed are kept in memory but not saved.");
				return;
			}
			writer.submit(name, serialized instanceof Serialized.Bytes(Value value1) ? value1 : null);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public boolean flush() {
		CsvVariableWriter writer = this.writer;
		return writer != null && !closed.get() && writer.flush(FLUSH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
	}

	@Override
	public void close() {
		if (!closed.compareAndSet(false, true))
			return;

		// Wait for any locks still being held.
		for (int i = 0; i < setLocks.size(); i++) {
			Lock lock = setLocks.getAt(i);
			lock.lock();
			lock.unlock();
		}

		CsvVariableWriter writer = this.writer;
		if (writer == null) {
			releaseFile();
			return;
		}
		switch (writer.stop(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
			// on FAILED, the writer has already reported what was lost
			case SAVED, FAILED -> releaseFile();
			// Keep the file registered, the writer may still be using it.
			case TIMED_OUT -> Skript.error("Timed out after " + CLOSE_TIMEOUT_SECONDS + " seconds waiting for database '"
				+ getUserConfigurationName() + "' to finish writing. Recent changes may be lost.");
		}
	}

	@Override
	protected boolean requiresFile() {
		return true;
	}

	@Override
	protected File getFile(String fileName) {
		return new File(fileName);
	}

	/**
	 * Collects per-variable load problems and prints one summary instead of one line each.
	 */
	private static final class LoadReport {

		private static final int EXAMPLES = 5;

		private final Map<String, Integer> counts = new LinkedHashMap<>();
		private final Map<String, List<String>> examples = new HashMap<>();

		/**
		 * Add an error.
		 *
		 * @param problem The problem type to report
		 * @param example A string to use for an example, usually a variable name.
		 */
		void add(String problem, String example) {
			counts.merge(problem, 1, Integer::sum);
			List<String> shown = examples.computeIfAbsent(problem, key -> new ArrayList<>(EXAMPLES));
			if (shown.size() < EXAMPLES)
				shown.add(example);
		}

		/**
		 * @return Whether the error count is empty.
		 */
		boolean isEmpty() {
			return counts.isEmpty();
		}

		/**
		 * Log this report as an error, using the given filename in the error.
		 *
		 * @param fileName The filename to use in the error.
		 */
		void print(String fileName) {
			if (counts.isEmpty())
				return;
			int total = counts.values().stream().mapToInt(Integer::intValue).sum();
			Skript.error("Encountered " + total + " problem" + (total == 1 ? "" : "s") + " while loading " + fileName
				+ " (the file will be backed up, then rewritten without the problematic variables):");
			counts.forEach((problem, count) -> {
				List<String> shown = examples.get(problem);
				String more = count > shown.size() ? ", and " + (count - shown.size()) + " more" : "";
				Skript.error("  " + count + " " + problem + ": " + String.join(", ", shown) + more);
			});
		}

	}

	// ============================ HELPERS ============================

	/**
	 * Serializes a value to bytes, or returns a special value indicating that the variable should be deleted or that serialization failed.
	 *
	 * @param value the value of the variable, or null if the variable is being deleted
	 * @return a Serialized object representing the result of the serialization
	 */
	@Contract("_ -> new")
	private static Serialized trySerialize(@Nullable Object value) {
		if (value == null)
			return new Serialized.Delete();
		Set<SerializedVariable> result;
		try {
			// singletonMap allows a null value; Variables.serialize handles the main-thread hop.
			result = Variables.serialize(Collections.singletonMap("", value));
		} catch (RuntimeException exception) {
			return new Serialized.Failed(exception);
		}
		if (result == null) // IOException inside a serializer, or Skript disabled while a main-thread hop was needed
			return new Serialized.Failed(null);
		SerializedVariable.Value bytes = result.iterator().next().value();
		return bytes == null ? new Serialized.Delete() : new Serialized.Bytes(bytes);
	}

	/**
	 * Logs a serialization failure once per name, and stops logging after {@link #MAX_REPORTED_FAILURES} names.
	 * Helps with preventing error spam on loading fails.
	 *
	 * @param name  The variable name that failed.
	 * @param cause an exception produced by the failure
	 */
	private void reportFailure(String name, @Nullable Throwable cause) {
		if (reportedFailures.contains(name))
			return;
		if (reportedFailures.size() >= MAX_REPORTED_FAILURES) {
			failuresSuppressed.compareAndSet(false, true);
			return;
		}
		if (!reportedFailures.add(name))
			return;
		String message = "Cannot serialize variable '" + name + "'; keeping its last saved value. "
			+ "Further failures for this variable are not logged.";
		if (cause != null)
			Skript.exception(cause, message);
		else
			Skript.error(message);
	}


	/**
	 * Returns the proper lock for a variable name.
	 * Locks based on the hash of the first segment of the variable name, so that variables with the same prefix share a lock.
	 *
	 * @param name the variable name
	 * @return the appropriate lock for the variable name
	 */
	private @NonNull Lock getLockFor(@NonNull String name) {
		int end = name.indexOf(Variable.SEPARATOR);
		int length = end == -1 ? name.length() : end;
		int hash = 0;
		for (int i = 0; i < length; i++)
			hash = 31 * hash + name.charAt(i);
		hash ^= hash >>> 16;
		return setLocks.getAt(hash & (setLocks.size() - 1));
	}

	/**
	 * Checks if file ends with newline
	 *
	 * @param path Path to the file.
	 * @return Whether the file ends with a newline char.
	 */
	private static boolean endsWithNewline(Path path) throws IOException {
		try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
			long size = channel.size();
			if (size == 0)
				return true;
			ByteBuffer last = ByteBuffer.allocate(1);
			channel.read(last, size - 1);
			return last.get(0) == '\n';
		}
	}

}
