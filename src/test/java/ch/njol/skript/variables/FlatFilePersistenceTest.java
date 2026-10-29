package ch.njol.skript.variables;

import ch.njol.skript.Skript;
import ch.njol.skript.config.Config;
import ch.njol.skript.config.EntryNode;
import ch.njol.skript.config.SectionNode;
import ch.njol.skript.log.BlockingLogHandler;
import ch.njol.skript.log.LogEntry;
import ch.njol.skript.log.LogHandler;
import ch.njol.skript.log.SkriptLogger;
import ch.njol.skript.registrations.Classes;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.skriptlang.skript.variables.CsvFormat;
import org.skriptlang.skript.variables.storage.flatfile.FlatFileStorage;

import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;
import java.util.logging.Level;

import static org.junit.Assert.*;

public class FlatFilePersistenceTest {

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	// ======================================== Helpers ========================================

	private SectionNode configuration(Path file, String... extra) throws Exception {
		Config config;
		try (BlockingLogHandler ignored = new BlockingLogHandler().start()) { // hides "'csv-test' is empty"
			config = new Config(new ByteArrayInputStream(new byte[0]), "csv-test", false, false, ":");
		}
		SectionNode node = new SectionNode("csv-test", "", config.getMainNode(), 0);
		node.add(new EntryNode("pattern", ".*", node));
		node.add(new EntryNode("file", file.toString(), node));
		node.add(new EntryNode("backup interval", "0", node));
		node.add(new EntryNode("required changes per save", "1000000", node));
		for (int i = 0; i + 1 < extra.length; i += 2)
			node.set(extra[i], extra[i + 1]);
		return node;
	}

	/**
	 * Collects log messages instead of printing them. Only sees messages logged on the installing thread.
	 */
	private static final class CapturingLogHandler extends LogHandler {

		private final List<LogEntry> entries = new ArrayList<>();

		@Override
		public LogResult log(LogEntry entry) {
			entries.add(entry);
			return LogResult.DO_NOT_LOG;
		}

		@Override
		public CapturingLogHandler start() {
			SkriptLogger.startLogHandler(this);
			return this;
		}

	}

	@FunctionalInterface
	private interface ThrowingSupplier<T> {
		T get() throws Exception;
	}

	/**
	 * Result of an action and the messages it logged on this thread.
	 */
	private record Logged<T>(T result, List<LogEntry> entries) {

		boolean contains(String text) {
			return entries.stream().anyMatch(entry -> entry.getMessage().contains(text));
		}

		long errors() {
			return entries.stream().filter(entry -> entry.getLevel().intValue() >= Level.SEVERE.intValue()).count();
		}

		void assertContains(String text) {
			assertTrue("expected a message containing '" + text + "', got "
				+ entries.stream().map(LogEntry::getMessage).toList(), contains(text));
		}

	}

	/**
	 * Runs an action that is expected to log, so expected errors are asserted instead of printed.
	 */
	private static <T> Logged<T> logged(ThrowingSupplier<T> action) throws Exception {
		CapturingLogHandler handler = new CapturingLogHandler().start();
		try {
			return new Logged<>(action.get(), List.copyOf(handler.entries));
		} finally {
			handler.stop();
		}
	}

	private FlatFileStorage open(Path file, String... extra) throws Exception {
		FlatFileStorage storage = new FlatFileStorage(Skript.instance(), "csv");
		assertTrue(storage.loadConfig(configuration(file, extra)));
		return storage;
	}

	private Path file(String name) {
		return folder.getRoot().toPath().resolve(name);
	}

	/**
	 * Encoded value of a string, as written in the file.
	 */
	private static String hex(String value) {
		SerializedVariable.Value serialized = Classes.serialize(value);
		assertNotNull(serialized);
		return CsvFormat.encode(serialized.data());
	}

	/**
	 * Variable rows (non-comment, non-empty lines) of the file whose name equals {@code name}.
	 */
	private static List<String[]> rows(Path file, String name) throws Exception {
		List<String[]> rows = new ArrayList<>();
		for (String line : Files.readAllLines(file, CsvFormat.CHARSET)) {
			line = line.trim();
			if (line.isEmpty() || line.startsWith("#"))
				continue;
			String[] split = CsvFormat.splitCSV(line);
			if (split != null && split.length == 3 && split[0].equals(name))
				rows.add(split);
		}
		return rows;
	}

	private static int countFiles(Path directory) throws Exception {
		if (!Files.isDirectory(directory))
			return 0;
		try (var files = Files.list(directory)) {
			return (int) files.count();
		}
	}

	private static void await(BooleanSupplier condition, String message) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
		while (!condition.getAsBoolean()) {
			if (System.nanoTime() > deadline)
				fail(message);
			Thread.sleep(10);
		}
	}

	// ======================================== Round trips ========================================

	@Test
	public void csvValuesSurviveReopen() throws Exception {
		Path file = file("variables.csv");
		try (FlatFileStorage first = open(file)) {
			first.setVariable("quoted, name", "value");
			first.setVariable("list::1", 42L);
			first.setVariable("list::2", 43L);
			assertTrue(first.flush());
		}
		try (FlatFileStorage second = open(file)) {
			assertEquals("value", second.getVariable("quoted, name"));
			assertEquals(42L, second.getVariable("list::1"));
			second.setVariable("list::*", null);
		}
		try (FlatFileStorage third = open(file)) {
			assertEquals("value", third.getVariable("quoted, name"));
			assertNull(third.getVariable("list::1"));
			assertNull(third.getVariable("list::2"));
		}
	}

	@Test
	public void closeWritesChangesThatWereNeverFlushed() throws Exception {
		Path file = file("close.csv");
		FlatFileStorage storage = open(file);
		storage.setVariable("entry", "final");
		storage.close();
		try (FlatFileStorage reopened = open(file)) {
			assertEquals("final", reopened.getVariable("entry"));
		}
	}

	@Test
	public void closeCompactsTheFile() throws Exception {
		Path file = file("close-compact.csv");
		FlatFileStorage storage = open(file);
		for (int i = 0; i < 20; i++) {
			storage.setVariable("entry", "value " + i);
			assertTrue(storage.flush()); // one row per flush, so the file holds superseded rows
		}
		assertTrue(rows(file, "entry").size() > 1);
		storage.close();
		List<String[]> rows = rows(file, "entry");
		assertEquals(1, rows.size());
		assertEquals(hex("value 19"), rows.getFirst()[2]);
	}

	@Test
	public void setAfterCloseIsKeptInMemoryButNotSaved() throws Exception {
		Path file = file("after-close.csv");
		FlatFileStorage storage = open(file);
		storage.close();
		logged(() -> {
			storage.setVariable("late", "value");
			return null;
		}).assertContains("closed");
		assertEquals("late value kept in memory", "value", storage.getVariable("late"));
		assertFalse(storage.flush());
		try (FlatFileStorage reopened = open(file)) {
			assertNull(reopened.getVariable("late"));
		}
	}

	// ======================================== Crash safety ========================================

	/**
	 * A copy taken after flush() and before close() is what a crash would leave on disk.
	 */
	@Test
	public void flushedChangesSurviveACrash() throws Exception {
		Path file = file("crash.csv");
		Path crashed = file("crash-copy.csv");
		try (FlatFileStorage storage = open(file)) {
			storage.setVariable("entry", "first");
			storage.setVariable("entry", "last");
			storage.setVariable("list::1", "one");
			storage.setVariable("list::2", "two");
			storage.setVariable("list::*", null);
			storage.setVariable("list::2", "restored");
			storage.setVariable("deleted", "old");
			assertTrue(storage.flush());
			storage.setVariable("deleted", null);
			assertTrue(storage.flush());
			Files.copy(file, crashed);
		}
		try (FlatFileStorage reopened = open(crashed)) {
			assertEquals("last", reopened.getVariable("entry"));
			assertNull(reopened.getVariable("list::1"));
			assertEquals("restored", reopened.getVariable("list::2"));
			assertNull("deletes must be written, or deleted variables return after a crash",
				reopened.getVariable("deleted"));
		}
	}

	@Test
	public void tornLastRowIsSkippedAndNotAppendedTo() throws Exception {
		Path file = file("torn.csv");
		// The last row has no newline, as if the process died mid-append.
		Files.writeString(file, "entry, string, " + hex("kept") + "\npartial, str");
		Logged<FlatFileStorage> load = logged(() -> open(file));
		load.assertContains("incomplete last rows");
		try (FlatFileStorage storage = load.result()) {
			assertEquals("kept", storage.getVariable("entry"));
			assertNull(storage.getVariable("partial"));
			storage.setVariable("next", "appended");
			assertTrue(storage.flush());
			assertTrue("the torn tail must be rewritten before appending",
				Files.readString(file).endsWith(System.lineSeparator()));
		}
		try (FlatFileStorage reopened = open(file)) {
			assertEquals("kept", reopened.getVariable("entry"));
			assertEquals("appended", reopened.getVariable("next"));
		}
		assertEquals("a recovery backup is made before rewriting", 1, countFiles(file.resolveSibling("backups")));
	}

	@Test
	public void tornLastRowWithParsableValueDoesNotReplacePreviousValue() throws Exception {
		Path file = file("torn-value.csv");
		String replacement = hex("replacement");
		// Torn inside the value at an even length, so the hex still decodes.
		Files.writeString(file, "entry, string, " + hex("kept") + "\nentry, string, " + replacement.substring(0, 8));
		Logged<FlatFileStorage> load = logged(() -> open(file));
		load.assertContains("incomplete last rows");
		try (FlatFileStorage storage = load.result()) {
			assertEquals("kept", storage.getVariable("entry"));
		}
		try (FlatFileStorage reopened = open(file)) {
			assertEquals("the rewrite keeps the previous value", "kept", reopened.getVariable("entry"));
		}
	}

	@Test
	public void tornLastRowWithEmptyValueDoesNotReplacePreviousValue() throws Exception {
		Path file = file("torn-empty.csv");
		// Torn right after the last separator, so the value is empty and decodes to no bytes.
		Files.writeString(file, "entry, string, " + hex("kept") + "\nentry, string, ");
		Logged<FlatFileStorage> load = logged(() -> open(file));
		load.assertContains("incomplete last rows");
		try (FlatFileStorage storage = load.result()) {
			assertEquals("kept", storage.getVariable("entry"));
		}
	}

	// ======================================== File format ========================================

	@Test
	public void listClearIsWrittenAsOneRow() throws Exception {
		Path file = file("list-clear.csv");
		try (FlatFileStorage storage = open(file)) {
			for (int i = 0; i < 100; i++)
				storage.setVariable("list::" + i, (long) i);
			storage.setVariable("list", "parent value");
			storage.setVariable("other", "untouched");
			assertTrue(storage.flush());
			storage.setVariable("list::*", null);
			assertTrue(storage.flush());

			assertEquals(1, rows(file, "list::*").size());
			assertEquals(CsvFormat.DELETED, rows(file, "list::*").getFirst()[1]);
			for (int i = 0; i < 100; i++)
				assertEquals("children must not get their own delete rows", 1, rows(file, "list::" + i).size());
		}
		try (FlatFileStorage reopened = open(file)) {
			assertNull(reopened.getVariable("list::5"));
			assertEquals("a list clear keeps the list's own value", "parent value", reopened.getVariable("list"));
			assertEquals("untouched", reopened.getVariable("other"));
		}
		assertTrue("final compaction drops the cleared rows", rows(file, "list::5").isEmpty());
	}

	@Test
	public void loaderAppliesRowsInFileOrder() throws Exception {
		Path file = file("order.csv");
		Files.writeString(file, String.join("\n",
			"list::a, string, " + hex("before clear"),
			"list::nested::x, string, " + hex("nested before clear"),
			"list, string, " + hex("parent"),
			"listless, string, " + hex("other list"),
			"list::*, null, ",
			"list::b, string, " + hex("after clear"),
			"entry, string, " + hex("first"),
			"entry, string, " + hex("last"),
			"deleted, string, " + hex("old"),
			"deleted, null, ",
			""));
		try (FlatFileStorage storage = open(file)) {
			assertNull(storage.getVariable("list::a"));
			assertNull(storage.getVariable("list::nested::x"));
			assertEquals("after clear", storage.getVariable("list::b"));
			assertEquals("parent", storage.getVariable("list"));
			assertEquals("other list", storage.getVariable("listless"));
			assertEquals("last", storage.getVariable("entry"));
			assertNull(storage.getVariable("deleted"));
		}
	}

	@Test
	public void burstOfChangesToOneVariableIsMerged() throws Exception {
		Path file = file("counter.csv");
		try (FlatFileStorage storage = open(file)) {
			for (long i = 1; i <= 1000; i++)
				storage.setVariable("counter", i);
			assertTrue(storage.flush());
			int rows = rows(file, "counter").size();
			// Exact batching depends on timing; without merging this would be 1000 rows.
			assertTrue("expected merged rows, got " + rows, rows < 20);
			assertEquals(1000L, storage.getVariable("counter"));
		}
		try (FlatFileStorage reopened = open(file)) {
			assertEquals(1000L, reopened.getVariable("counter"));
		}
	}

	@Test
	public void supersededRowsTriggerARewrite() throws Exception {
		Path file = file("compaction.csv");
		try (FlatFileStorage storage = open(file, "required changes per save", "10")) {
			for (int i = 0; i < 30; i++) {
				storage.setVariable("entry", "value " + i);
				assertTrue(storage.flush());
			}
			// The rewrite check runs when the writer wakes, so one more change triggers it.
			storage.setVariable("entry", "final");
			assertTrue(storage.flush());
			int rows = rows(file, "entry").size();
			assertTrue("expected a rewrite to drop superseded rows, got " + rows, rows < 31);
		}
	}

	// ======================================== Concurrency ========================================

	@Test
	public void concurrentSetsPersistTheSameFinalStateAsMemory() throws Exception {
		Path file = file("concurrent.csv");
		int threads = 8;
		int perThread = 500;
		FlatFileStorage storage = open(file);
		Map<String, Object> expected = new ConcurrentHashMap<>();
		ExecutorService executor = Executors.newFixedThreadPool(threads);
		try {
			CountDownLatch start = new CountDownLatch(1);
			List<Future<?>> futures = new ArrayList<>();
			for (int t = 0; t < threads; t++) {
				int thread = t;
				futures.add(executor.submit(() -> {
					start.await();
					for (int i = 0; i < perThread; i++) {
						storage.setVariable("own::" + thread + "::" + i, (long) i);
						storage.setVariable("shared::" + (i % 10), thread + ":" + i); // contended names
						if (i % 50 == 0)
							storage.setVariable("shared::*", null); // contended list clears
					}
					return null;
				}));
			}
			start.countDown();
			for (Future<?> future : futures)
				future.get(30, TimeUnit.SECONDS);
			assertTrue(storage.flush());

			for (int t = 0; t < threads; t++) {
				for (int i = 0; i < perThread; i++)
					expected.put("own::" + t + "::" + i, (long) i);
			}
			for (int i = 0; i < 10; i++) {
				Object value = storage.getVariable("shared::" + i);
				if (value != null)
					expected.put("shared::" + i, value);
			}
		} finally {
			executor.shutdownNow();
			storage.close();
		}

		try (FlatFileStorage reopened = open(file)) {
			expected.forEach((name, value) -> assertEquals(name, value, reopened.getVariable(name)));
			for (int i = 0; i < 10; i++) {
				String name = "shared::" + i;
				assertEquals("file and memory must agree for " + name, expected.get(name), reopened.getVariable(name));
			}
		}
	}

	// ======================================== Damaged files ========================================

	@Test
	public void damagedRowsAreSummarizedSkippedAndBackedUp() throws Exception {
		Path file = file("damaged.csv");
		StringBuilder text = new StringBuilder("good, string, " + hex("value") + "\n");
		for (int i = 0; i < 1000; i++)
			text.append("broken ").append(i).append(", \"unterminated\n"); // invalid CSV
		text.append("tags::*, string, ").append(hex("list name")).append('\n'); // list name with a value
		text.append("badhex, string, XYZ\n"); // unreadable value
		Files.writeString(file, text);
		Logged<FlatFileStorage> load = logged(() -> open(file));
		assertTrue("1002 damaged rows must be summarized, got " + load.errors() + " errors", load.errors() < 20);
		load.assertContains("1000 invalid rows");
		load.assertContains("list variables holding a value");
		load.assertContains("unreadable values");
		try (FlatFileStorage storage = load.result()) {
			assertEquals("value", storage.getVariable("good"));
			assertNull(storage.getVariable("badhex"));
			assertTrue(storage.flush());
		}
		assertEquals(1, countFiles(file.resolveSibling("backups")));
		assertFalse("damaged rows are dropped by the rewrite", Files.readString(file).contains("unterminated"));
		assertEquals(1, rows(file, "good").size());
	}

	@Test
	public void undeserializableValuesAreKeptInTheFile() throws Exception {
		Path file = file("unknown-type.csv");
		Files.writeString(file, "mystery, no-such-type, 0102\nknown, string, " + hex("value") + "\n");
		Logged<FlatFileStorage> load = logged(() -> open(file));
		load.assertContains("mystery");
		try (FlatFileStorage storage = load.result()) {
			assertNull(storage.getVariable("mystery"));
			assertEquals("value", storage.getVariable("known"));
			storage.setVariable("known", "changed");
		}
		List<String[]> rows = rows(file, "mystery");
		assertEquals("a value of an unavailable type survives for when its addon returns", 1, rows.size());
		assertEquals("no-such-type", rows.getFirst()[1]);
	}

	@Test
	public void invalidVersionHeaderIsIgnored() throws Exception {
		Path file = file("bad-version.csv");
		Files.writeString(file, "# version: not a version\nentry, string, " + hex("value") + "\n");
		Logged<FlatFileStorage> load = logged(() -> open(file));
		load.assertContains("version");
		try (FlatFileStorage storage = load.result()) {
			assertEquals("value", storage.getVariable("entry"));
		}
	}

	@Test
	public void duplicateRowsUseLastValueAndDeletion() throws Exception {
		Path file = file("duplicates.csv");
		Files.writeString(file, "entry, string, " + hex("first") + "\nentry, null, \n");
		try (FlatFileStorage storage = open(file)) {
			assertNull(storage.getVariable("entry"));
		}
	}

	// ======================================== Backups ========================================

	private FlatFileStorage openWithBackups(Path file, Path backupDirectory) throws Exception {
		FlatFileStorage storage = new FlatFileStorage(Skript.instance(), "csv") {
			@Override
			protected Path getBackupDirectory() {
				return backupDirectory;
			}
		};
		assertTrue(storage.loadConfig(configuration(file)));
		return storage;
	}

	@Test
	public void backupsRequireChangesAndZeroRetentionRemovesExistingBackups() throws Exception {
		Path directory = folder.newFolder("retention-zero").toPath();
		long before = System.currentTimeMillis();
		try (FlatFileStorage storage = openWithBackups(file("no-change.csv"), directory)) {
			assertTrue("the first backup waits one interval", storage.lastBackup >= before);
			storage.backupIntervalMillis = 1;
			storage.lastBackup = 0;
			assertFalse("no backup without changes", storage.isBackupDue());

			storage.setVariable("entry", "value");
			assertTrue(storage.flush());
			await(() -> storage.lastBackup != 0, "the writer should take a backup after a change");
			assertEquals(1, countFiles(directory));

			storage.lastBackup = 0;
			Thread.sleep(200);
			assertFalse("no new backup without new changes", storage.isBackupDue());
			assertEquals(1, countFiles(directory));

			Files.writeString(directory.resolve("1-00000000-0000-0000-0000-000000000000.csv"), "old");
			Files.writeString(directory.resolve("keep.txt"), "unrelated");
			storage.backupIntervalMillis = 0;
			storage.backupsToKeep = 0;
			storage.backupIfDue();
			try (var files = Files.list(directory)) {
				assertEquals(List.of(directory.resolve("keep.txt")), files.toList());
			}
		}
	}

	@Test
	public void backupRetentionPreservesUnrelatedFiles() throws Exception {
		Path directory = folder.newFolder("backups-retained").toPath();
		try (FlatFileStorage storage = openWithBackups(file("backed.csv"), directory)) {
			storage.backupIntervalMillis = 1;
			storage.backupsToKeep = 2;
			Path unrelated = directory.resolve("keep.txt");
			Files.writeString(unrelated, "untouched");
			for (int i = 0; i < 4; i++) {
				storage.lastBackup = 0;
				storage.setVariable("value", "value " + i);
				assertTrue(storage.flush());
				await(() -> storage.lastBackup != 0, "backup " + i + " was not taken");
			}
			// lastBackup is set before old backups are pruned, so wait for the prune as well.
			await(() -> {
				try {
					return countFiles(directory) == 3;
				} catch (Exception exception) {
					throw new RuntimeException(exception);
				}
			}, "expected two backups plus the unrelated file");
			assertEquals("untouched", Files.readString(unrelated));
		}
	}

	@Test
	public void backupsAreCompleteSnapshots() throws Exception {
		Path directory = folder.newFolder("snapshots").toPath();
		Path file = file("snapshot.csv");
		FlatFileStorage storage = openWithBackups(file, directory);
		List<String> written = new ArrayList<>();
		try {
			for (int i = 0; i < 5; i++) {
				written.add(hex("value " + i));
				storage.setVariable("entry", "value " + i);
				assertTrue(storage.flush()); // leaves superseded rows in the live file
			}
			written.add(hex("backed up"));
			// The writer may still be finishing the last flush's loop when backups are enabled, so the
			// backup can be of any snapshot taken from here on. What matters is that it is complete.
			storage.backupIntervalMillis = 1;
			storage.lastBackup = 0;
			storage.setVariable("entry", "backed up");
			assertTrue(storage.flush());
			await(() -> storage.lastBackup != 0, "the writer should take a backup");
		} finally {
			storage.backupIntervalMillis = 0;
			storage.close();
		}
		Path backup;
		try (var files = Files.list(directory)) {
			backup = files.findFirst().orElseThrow();
		}
		byte[] bytes = Files.readAllBytes(backup);
		if (backup.toString().endsWith(".gz")) {
			try (var gzip = new java.util.zip.GZIPInputStream(new java.io.ByteArrayInputStream(bytes))) {
				bytes = gzip.readAllBytes();
			}
		}
		Path plain = file("backup-content.csv");
		Files.write(plain, bytes);
		List<String[]> rows = rows(plain, "entry");
		assertEquals("backups are taken right after a rewrite, with one row per variable", 1, rows.size());
		assertTrue("the backup holds a value that was written", written.contains(rows.getFirst()[2]));
	}

	@Test
	public void backupFailureDoesNotPreventFlushOrFinalSave() throws Exception {
		Path file = file("backup-failure.csv");
		Path directory = file("failed-backups");
		FlatFileStorage failing = new FlatFileStorage(Skript.instance(), "csv") {
			@Override
			protected Path getBackupDirectory() {
				return directory;
			}

			@Override
			protected void writeBackup(Path target) throws java.io.IOException {
				throw new java.io.IOException("Injected backup failure");
			}
		};
		try (failing) {
			assertTrue(failing.loadConfig(configuration(file)));
			failing.backupIntervalMillis = 1;
			failing.lastBackup = 0;
			failing.setVariable("entry", "flushed");
			assertTrue(failing.flush());
			assertEquals(hex("flushed"), rows(file, "entry").getLast()[2]);
			failing.setVariable("entry", "final");
		} finally {
			failing.backupIntervalMillis = 0;
		}
		try (FlatFileStorage reopened = open(file)) {
			assertEquals("final", reopened.getVariable("entry"));
		}
	}

	@Test
	public void flatFileBackupIsGzipAndPreservesFileBytes() throws Exception {
		Path file = file("compressed.csv");
		FlatFileStorage storage = new FlatFileStorage(Skript.instance(), "csv") {
			@Override
			protected String backupExtension() {
				return ".csv.gz";
			}
		};
		try (storage) {
			assertTrue(storage.loadConfig(configuration(file)));
			storage.setVariable("entry", "hello");
			assertTrue(storage.flush());
			Path backup = file("snapshot.csv.gz");
			storage.writeBackup(backup);
			try (var gzip = new java.util.zip.GZIPInputStream(Files.newInputStream(backup))) {
				assertArrayEquals(Files.readAllBytes(file), gzip.readAllBytes());
			}
		}
	}

	// ======================================== Configuration ========================================

	@Test
	public void invalidSavePeriodDoesNotLoad() throws Exception {
		FlatFileStorage storage = new FlatFileStorage(Skript.instance(), "csv");
		SectionNode config = configuration(file("invalid.csv"));
		config.set("save period", "0");
		try {
			Logged<Boolean> load = logged(() -> storage.loadConfig(config));
			assertFalse(load.result());
			load.assertContains("save period");
		} finally {
			storage.close();
		}
	}

	@Test
	public void failedDuplicateLoadDoesNotReleaseAnotherStoragesFile() throws Exception {
		Path file = file("shared.csv");
		try (FlatFileStorage owner = open(file)) {
			for (int i = 0; i < 2; i++) {
				try (FlatFileStorage duplicate = new FlatFileStorage(Skript.instance(), "csv")) {
					Logged<Boolean> load = logged(() -> duplicate.loadConfig(configuration(file)));
					assertFalse(load.result());
					load.assertContains("already registered");
				}
			}
			owner.setVariable("entry", "value");
			assertTrue(owner.flush());
		}
	}

}
