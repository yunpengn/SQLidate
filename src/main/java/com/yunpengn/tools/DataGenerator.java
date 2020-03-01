package com.yunpengn.tools;

import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Random;

public class DataGenerator {
  private static final Path OUTPUT = Paths.get("scripts/insert_data.sql");
  private static final Random RANDOM = new Random();

  public void run() throws Exception {
    final Writer writer = Files.newBufferedWriter(OUTPUT);
    String query = "";
    assert query.equals(query.toLowerCase());

    query = generate("a", 5, 0, 20);
    writer.write(query);
    query = generate("b", 10, 0, 40);
    writer.write(query);
    query = generate("c", 50, -50, 50);
    writer.write(query);
    query = generate("d", 150, -100, 200);
    writer.write(query);
    query = generate("e", 200, -100, 500);
    writer.write(query);
    query = generate("f", 800, -500, 1000);
    writer.write(query);
    query = generate("g", 1500, -500, 3000);
    writer.write(query);
    query = generate("h", 6000, -500, 10000);
    writer.write(query);

    writer.flush();
    writer.close();
  }

  private String generate(final String tableName, final int numRows, final int upper, final int lower) {
    System.out.printf("Going to generate INSERT query for table %s.\n", tableName);

    final StringBuilder builder = new StringBuilder();
    builder.append(String.format("INSERT INTO %s (\"%sID\") VALUES ", tableName, tableName));

    // Inserts data.
    for (int i = 0; i < numRows; i++) {
      final int num = RANDOM.nextInt(upper - lower) + lower;
      builder.append(String.format("(%d)", num));
      builder.append(i == numRows - 1 ? ", " : ";");
    }
    builder.append("\n\n");

    return builder.toString();
  }
}
