package ch.njol.skript.variables;

import org.junit.Test;
import org.skriptlang.skript.variables.CsvFormat;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.TreeMap;

import static org.junit.Assert.*;

public class CsvFormatTest {

	@Test
	public void testHexCoding() {
		byte[] bytes = {-0x80, -0x50, -0x01, 0x00, 0x01, 0x44, 0x7F};
		String string = "80B0FF0001447F";
		assertEquals(string, CsvFormat.encode(bytes));
		assertArrayEquals(bytes, CsvFormat.decode(string));
		assertArrayEquals(bytes, CsvFormat.decode(string.toLowerCase()));
	}

	@Test
	public void rejectsMalformedHexadecimal() {
		assertThrows(IllegalArgumentException.class, () -> CsvFormat.decode("F"));
		assertThrows(IllegalArgumentException.class, () -> CsvFormat.decode("GG"));
	}

	@Test
	public void testSplitCSV() {
		String[][] vs = {
			{"", ""},
			{",", "", ""},
			{",,", "", "", ""},
			{"a", "a"},
			{"a,", "a", ""},
			{",a", "", "a"},
			{",a,", "", "a", ""},
			{" , a , ", "", "a", ""},
			{"a,b,c", "a", "b", "c"},
			{" a , b , c ", "a", "b", "c"},

			{"\"\"", ""},
			{"\",\"", ","},
			{"\"\"\"\"", "\""},
			{"\" \"", " "},
			{"a, \"\"\"\", b, \", c\", d", "a", "\"", "b", ", c", "d"},
			{"a, \"\"\", b, \", c", "a", "\", b, ", "c"},

			{"\"\t\0\"", "\t\0"},
		};
		for (String[] v : vs) {
			String[] expected = Arrays.copyOfRange(v, 1, v.length);
			assertArrayEquals(v[0], expected, CsvFormat.splitCSV(v[0]));
		}
	}

	@Test
	public void writtenRowsSplitBackIntoTheSameValues() {
		String[] names = {"plain", "with, comma", "with \"quotes\"", "with # hash", "with space", "list::*"};
		for (String name : names) {
			StringWriter text = new StringWriter();
			try (PrintWriter writer = new PrintWriter(text)) {
				CsvFormat.writeRow(writer, name, new SerializedVariable.Value("string", new byte[]{1, 2, 3}));
				CsvFormat.writeRow(writer, name, null);
			}
			String[] lines = text.toString().split("\\R");
			assertArrayEquals(name, new String[]{name, "string", "010203"}, CsvFormat.splitCSV(lines[0]));
			assertArrayEquals(name, new String[]{name, CsvFormat.DELETED, ""}, CsvFormat.splitCSV(lines[1]));
		}
	}

	@Test
	public void listPrefixRangeContainsExactlyTheListElements() {
		TreeMap<String, Integer> map = new TreeMap<>();
		String[] inside = {"x::", "x::a", "x::yz", "x::a::b", "x::￿z", "x::😀"};
		String[] outside = {"x", "x:;", "x:a", "xa", "y::a", "w::a"};
		for (String key : inside)
			map.put(key, 1);
		for (String key : outside)
			map.put(key, 1);

		String prefix = CsvFormat.listPrefix("x::*");
		assertEquals("x::", prefix);
		assertEquals("x:;", CsvFormat.prefixEnd(prefix));
		assertEquals(new java.util.TreeSet<>(Arrays.asList(inside)),
			map.subMap(prefix, CsvFormat.prefixEnd(prefix)).keySet());
	}

}
