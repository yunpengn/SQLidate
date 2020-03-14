package com.yunpengn.tools;

import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

public class DataBigGenerator {
  private static final String FILE_FORMAT = "scripts/tables/%s.sql";
  private static final Random RANDOM = new Random();
  private static final int BATCH_SIZE = 100_000;

  public void run() throws Exception {
    generate("a", 1_000_000, 0, 100_000);
    generate("b", 1_000_000, 0, 100_000);
    generate("c", 1_000_000, 0, 100_000);
    generate("d", 1_000_000, 0, 100_000);
    generate("e", 1_000_000, 0, 100_000);
    generate("f", 1_000_000, 0, 100_000);
  }

  private void generate(final String tableName, final int numRows, final int lower, final int upper) throws Exception {
    final String fileName = String.format(FILE_FORMAT, tableName);
    final Path filePath = Paths.get(fileName);
    final Writer writer = Files.newBufferedWriter(filePath);
    System.out.printf("Going to generate INSERT query for table %s.\n", tableName);

    // Writes the content.
    final String prefix = String.format("INSERT INTO %s (\"%sID\") VALUES ", tableName, tableName);
    writer.write(prefix);
    for (int i = 1; i <= numRows; i++) {
      final int num = RANDOM.nextInt(upper - lower) + lower;
      writer.write("(" + num + ")");
      writer.write(i < numRows ? ", " : ";");

      if (i % BATCH_SIZE == 0) {
        System.out.printf("Have generated %d entries.\n", i);
      }
    }
    writer.write("\n\n");

    // Closes the writer.
    writer.flush();
    writer.close();

    // Separates the output.
    System.out.println("-----------------------------------------------------------\n");
  }
}
