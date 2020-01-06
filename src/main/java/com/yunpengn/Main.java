package com.yunpengn;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * The main driver class.
 */
public class Main {
  // The name for configuration file.
  private static final String PROPERTY_FILE_NAME = "config.properties";

  // The query used to check the difference of the results of two queries.
  private static final String META_QUERY = "((%s) EXCEPT (%s)) UNION ALL ((%s) EXCEPT (%s));";
  // The query used to wrap the input.
  private static final String WRAP_QUERY = "SELECT \"%s\" from (%s) AS \"z\"";

  // The field names to look for in the queries.
  private static final List<String> FIELD_NAMES = Arrays.asList(
      "aID", "bID", "cID", "dID", "eID", "fID", "gID", "hID", "iID", "jID", "kID", "lID"
  );

  // The default delimiter used in result output.
  private static final String PAIR_DELIMITER
      = "=============================================================";
  private static final String INTERNAL_DELIMITER
      = "-------------------------------------------------------------";
  private static final String LINE_DELIMITER = "\n";

  // The batch size used when printing progress bar.
  private static final int BATCH_SIZE = 50;

  // The names of the files to store logs.
  private static final Path OUT_PATH = Paths.get("out.log");
  private static final Path ERR_PATH = Paths.get("out.err.log");

  // The description for output.
  private static final String WRONG_DESC = "The following 2 queries are not equivalent.";
  private static final String ERROR_DESC = "Meet exception when comparing the following 2 queries: %s.";

  /**
   * The main function.
   *
   * @param args are the CLI arguments.
   */
  public static void main(String[] args) throws Exception {
    // Input validation.
    if (args.length == 0) {
      System.err.println("Usage: java -jar XXX.jar <input_file_path>");
      return;
    }
    final String inputFile = args[0];

    // Whether to enable query wrapper.
    boolean wrapInput = true;
    if (args.length > 1) {
      switch (args[1].toLowerCase()) {
      case "y":
      case "yes":
      case "true":
      case "t":
        wrapInput = true;
        break;
      case "n":
      case "no":
      case "false":
      case "f":
        wrapInput = false;
        break;
      default:
        System.err.println("Usage: java -jar XXX.jar <input_file_path> [Y/N]");
        return;
      }
    }

    // Creates the database connection & the output stream.
    final Connection connection = createConnection();
    final Writer outWriter = Files.newBufferedWriter(OUT_PATH);
    final Writer errWriter = Files.newBufferedWriter(ERR_PATH);

    // Reads the input.
    final Map<Pair, String> pairs = readInput(inputFile);

    // Compares each pair of queries.
    int count = 0;
    int wrongCount = 0;
    int errorCount = 0;
    for (Map.Entry<Pair, String> entry: pairs.entrySet()) {
      // Wraps the query to guarantee select ordering.
      String queryA = entry.getKey().first;
      if (wrapInput) {
        queryA = wrapQuery(queryA);
      }
      String queryB = entry.getKey().second;
      if (wrapInput) {
        queryB = wrapQuery(queryB);
      }

      // Checks the query.
      try {
        if (!compareQueryResult(connection, queryA, queryB)) {
          wrongCount++;
          printQueryPair(outWriter, entry.getKey(), WRONG_DESC, entry.getValue());
        }
      } catch (SQLException e) {
        errorCount++;

        final String desc = String.format(ERROR_DESC, e);
        printQueryPair(errWriter, entry.getKey(), desc, entry.getValue());
      }

      // Prints the progress bar (if necessary).
      count++;
      if (count % BATCH_SIZE == 0) {
        System.out.printf("Progress: %d out of %d (%d wrong & %d error).\n",
            count, pairs.size(), wrongCount, errorCount);
      }
    }

    // Closes the database connection & output streams.
    connection.close();
    outWriter.flush();
    outWriter.close();
    errWriter.flush();
    errWriter.close();
  }

  /**
   * Reads the configuration file and creates the database connection.
   *
   * @return the database connection
   * @throws Exception when there is any I/O error or database error.
   */
  private static Connection createConnection() throws Exception {
    // Loads the property.
    final Properties props = new Properties();
    props.load(new FileInputStream(PROPERTY_FILE_NAME));

    // Creates the database connection.
    String dbName = props.getProperty("db");
    String url = "jdbc:postgresql://localhost/" + dbName;
    return DriverManager.getConnection(url, props);
  }

  /**
   * Checks whether the {@link ResultSet} of two queries are the same.
   *
   * @param connection is the database connection.
   * @param queryA is the first query.
   * @param queryB is the second query.
   * @return true if their results are the same.
   * @throws SQLException when there is any database error.
   */
  private static boolean compareQueryResult(Connection connection, String queryA, String queryB) throws SQLException {
    // Constructs the meta query.
    String query = String.format(META_QUERY, queryA, queryB, queryB, queryA);

    // Executes the meta query.
    Statement statement = connection.createStatement();
    ResultSet result = statement.executeQuery(query);
    boolean isEmpty = !result.next();
    statement.close();

    // Checks whether the result is empty.
    return isEmpty;
  }

  /**
   * Wraps an input query to guarantee the ordering in its SELECT clause.
   *
   * @param input is the input query.
   * @return the wrapped query.
   */
  private static String wrapQuery(String input) {
    String availableFields = FIELD_NAMES.stream()
        .filter(field -> input.contains(field) || input.contains("\"" + field.substring(0, 1) + "\""))
        .collect(Collectors.joining("\", \""));
    return String.format(WRAP_QUERY, availableFields, input);
  }

  /**
   * Reads input from a given file.
   *
   * @param fileName is the path to the given file.
   * @return all pairs of queries in that file.
   * @throws IOException when there is any I/O error.
   */
  private static Map<Pair, String> readInput(String fileName) throws IOException {
    // Creates the reader.
    FileReader fileReader = new FileReader(fileName);
    BufferedReader reader = new BufferedReader(fileReader);

    // Reads line by line.
    Map<Pair, String> result = new HashMap<>();
    while (PAIR_DELIMITER.equals(reader.readLine())) {
      String first = readUntil(reader, INTERNAL_DELIMITER);
      String second = readUntil(reader, INTERNAL_DELIMITER);
      String description = readUntil(reader, PAIR_DELIMITER);
      result.put(new Pair(first, second), description);
    }

    // Closes the reader and returns the result.
    fileReader.close();
    reader.close();
    return result;
  }

  /**
   * Reads from an input reader until EOF or we meet a line which contains a given delimiter.
   *
   * @param reader is the input reader.
   * @param delimiter is the given  delimiter.
   * @return the lines before the EOF or delimiter, concatenated together.
   * @throws IOException when there is any I/O error.
   */
  private static String readUntil(BufferedReader reader, String delimiter) throws IOException {
    StringBuilder result = new StringBuilder();

    // Reads line by line until we meet EOF or the delimiter.
    String currentLine = reader.readLine();
    while (currentLine != null && !currentLine.equals(delimiter)) {
      result.append(currentLine).append(LINE_DELIMITER);
      currentLine = reader.readLine();
    }

    // Returns the result.
    return result.toString();
  }

  /**
   * Prints out information about a pair of queries.
   *
   * @param writer is the given output stream.
   * @param pair is the pair of queries.
   * @param description is the description for this pair.
   * @param type is the type of the transformation.
   * @throws IOException when any I/O error happens.
   */
  private static void printQueryPair(Writer writer, Pair pair, String description, String type) throws IOException {
    writer.write(PAIR_DELIMITER + "\n");
    writer.write(description + "\n\n");
    writer.write("First query:\n\n");
    writer.write(pair.first + ";\n");
    writer.write(INTERNAL_DELIMITER + "\n");
    writer.write("Second query:\n\n");
    writer.write(pair.second + ";\n");
    writer.write(INTERNAL_DELIMITER + "\n");
    writer.write(type + "\n");
    writer.write(PAIR_DELIMITER + "\n");
  }
}
