package com.yunpengn.tools;

import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Map.Entry;

public class DataGenerator {
  private static final Path OUTPUT = Paths.get("scripts/insert_data.sql");
  private static final Map<String, Integer> tables = Map.of(
      "a", 1,
      "b", 1,
      "c", 1,
      "d", 1,
      "e", 1,
      "f", 1,
      "g", 1,
      "h", 1
  );

  private final int lower;
  private final int upper;

  public DataGenerator(final int lower, final int upper) {
    this.lower = lower;
    this.upper = upper;
  }

  public void run() throws Exception {
    final Writer writer = Files.newBufferedWriter(OUTPUT);

    for (final Entry<String, Integer> entry: tables.entrySet()) {
      final String query = generate(entry.getKey(), entry.getValue());
      writer.write(query);
      writer.write("\n\n");
    }

    writer.flush();
    writer.close();
  }

  private String generate(final String tableName, final int numRows) {
    return "";
  }
}
