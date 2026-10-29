package org.skriptlang.skript.variables;

import ch.njol.skript.Skript;
import ch.njol.skript.variables.SerializedVariable;
import org.jetbrains.annotations.Nullable;

import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utilities for the typical Skript CSV variable file format
 * Each row is one variable, formatted as {@code name, type, hexData}.
 * If the type is null, the variable has been deleted (and is waiting on compaction)
 */
public final class CsvFormat {

	/**
	 * The charset of CSV variable files.
	 */
	public static final Charset CHARSET = StandardCharsets.UTF_8;

	/**
	 * Type written for deletion rows.
	 */
	public static final String DELETED = "null";

	/**
	 * Prefix of the header line recording the Skript version that wrote the file.
	 */
	public static final String VERSION_HEADER = "# version:";

	private static final Pattern HEX_PATTERN = Pattern.compile("[0-9a-fA-F]*");

	/**
	 * A regex pattern of a line in a CSV file.
	 * <ul>
	 * <li>{@code (?<=^|,)}: assert that the match is preceded by the start of the line or a comma</li>
	 * <li>{@code (?:([^",]*)|"((?:[^"]+|"")*)")}: match either a quoted or unquoted value</li>
	 * <ul>
	 * 	<li>- {@code ([^",]*)}: match an unquoted value</li>
	 * 	<li>- {@code "((?:[^"]+|"")*)"}: match a quoted value</li>
	 * </ul>
	 * <li>{@code (?:,|$)}: match either a comma or the end of the line</li>
	 * </ul>
	 */
	private static final Pattern CSV_LINE_PATTERN = Pattern.compile("(?<=^|,)\\s*(?:([^\",]*)|\"((?:[^\"]+|\"\")*)\")\\s*(?:,|$)");

	/**
	 * A regex pattern to check if a string contains whitespace.
	 */
	private static final Pattern CONTAINS_WHITESPACE = Pattern.compile("\\s");

	private CsvFormat() {
		throw new UnsupportedOperationException();
	}

	/**
	 * Writes the file header.
	 *
	 * @param writer the writer
	 */
	public static void writeHeader(PrintWriter writer) {
		writer.println("# === Skript's variable storage ===");
		writer.println("# Please do not modify this file manually!");
		writer.println("#");
		writer.println(VERSION_HEADER + " " + Skript.getVersion());
		writer.println();
	}

	/**
	 * Writes one variable row, or a deletion row if {@code value} is null.
	 *
	 * @param writer the writer
	 * @param name   the variable name
	 * @param value  the serialized value, or null for a deletion
	 */
	public static void writeRow(PrintWriter writer, String name, SerializedVariable.@Nullable Value value) {
		writeCSV(writer, name,
			value == null ? DELETED : value.type(),
			value == null ? "" : encode(value.data()));
	}

	/**
	 * Returns the prefix shared by every element of a list variable.
	 *
	 * @param listName a list variable name ending in {@code ::*}, e.g. {@code a::b::*}
	 * @return the prefix, e.g. {@code a::b::}
	 */
	public static String listPrefix(String listName) {
		assert listName.endsWith("*") : listName;
		return listName.substring(0, listName.length() - 1);
	}

	/**
	 * Returns the exclusive upper bound of all strings starting with {@code prefix},
	 * for use with {@link java.util.NavigableMap#subMap(Object, Object)} on naturally ordered keys.
	 * <p>
	 * The last character is incremented, so {@code "a::"} becomes {@code "a:;"}: every string starting
	 * with the prefix sorts below it, and nothing else sorts between the prefix and the bound.
	 *
	 * @param prefix a non-empty prefix whose last character is not {@link Character#MAX_VALUE}
	 * @return the exclusive upper bound
	 */
	public static String prefixEnd(String prefix) {
		int last = prefix.length() - 1;
		assert last >= 0 && prefix.charAt(last) != Character.MAX_VALUE : prefix;
		return prefix.substring(0, last) + (char) (prefix.charAt(last) + 1);
	}

	/**
	 * Encode the given byte array to a hexadecimal string.
	 *
	 * @param data the byte array to encode.
	 * @return the hex string.
	 */
	public static String encode(byte[] data) {
		char[] encoded = new char[data.length * 2];

		for (int i = 0; i < data.length; i++) {
			encoded[2 * i] = Character.toUpperCase(Character.forDigit((data[i] & 0xF0) >>> 4, 16));
			encoded[2 * i + 1] = Character.toUpperCase(Character.forDigit(data[i] & 0xF, 16));
		}

		return new String(encoded);
	}

	/**
	 * Decodes the given hexadecimal string to a byte array.
	 *
	 * @param hex the hex string to encode.
	 * @return the byte array.
	 * @throws IllegalArgumentException if the string is not valid hexadecimal.
	 */
	public static byte[] decode(String hex) {
		if ((hex.length() & 1) != 0 || !HEX_PATTERN.matcher(hex).matches())
			throw new IllegalArgumentException("Invalid hexadecimal value");
		byte[] decoded = new byte[hex.length() / 2];

		for (int i = 0; i < decoded.length; i++) {
			decoded[i] = (byte) ((Character.digit(hex.charAt(2 * i), 16) << 4) + Character.digit(hex.charAt(2 * i + 1), 16));
		}

		return decoded;
	}

	/**
	 * Splits the given CSV line into its values.
	 *
	 * @param line the CSV line.
	 * @return the array of values, or null if the line is not valid CSV.
	 * @see #CSV_LINE_PATTERN
	 */
	public static String @Nullable [] splitCSV(String line) {
		Matcher matcher = CSV_LINE_PATTERN.matcher(line);

		int lastEnd = 0;
		ArrayList<String> result = new ArrayList<>();

		while (matcher.find()) {
			if (lastEnd != matcher.start())
				return null; // other stuff in between finds

			if (matcher.group(1) != null) {
				// unquoted, leave as is
				result.add(matcher.group(1).trim());
			} else {
				// quoted, remove quotes
				result.add(matcher.group(2).replace("\"\"", "\""));
			}

			lastEnd = matcher.end();
		}

		if (lastEnd != line.length())
			return null; // other stuff after last find

		return result.toArray(new String[0]);
	}

	/**
	 * Writes the given 3 values as a CSV value to the given {@link PrintWriter}.
	 *
	 * @param printWriter the print writer.
	 * @param values      the values, must have a length of {@code 3}.
	 */
	public static void writeCSV(PrintWriter printWriter, String... values) {
		assert values.length == 3; // name, type, value

		for (int i = 0; i < values.length; i++) {
			if (i != 0)
				printWriter.print(", ");

			String value = values[i];

			// Check if the value should be escaped
			boolean escapingNeeded = value != null
				&& (value.contains(",")
				|| value.contains("\"")
				|| value.contains("#")
				|| CONTAINS_WHITESPACE.matcher(value).find());
			if (escapingNeeded) {
				value = '"' + value.replace("\"", "\"\"") + '"';
			}

			printWriter.print(value);
		}

		printWriter.println();
	}

}
