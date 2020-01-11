package com.yunpengn.tools;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Compares the results of pairs of SQL queries.
 */
public class ResultVerifier {
  // The query used to check the difference of the results of two queries.
  private static final String META_QUERY = "((%s) EXCEPT (%s)) UNION ALL ((%s) EXCEPT (%s));";
  // The query used to wrap the input.
  private static final String WRAP_QUERY = "SELECT \"%s\" from (%s) AS \"z\"";

  // The field names to look for in the queries.
  private static final List<String> FIELD_NAMES = Arrays.asList(
      "aID", "bID", "cID", "dID", "eID", "fID", "gID", "hID", "iID", "jID", "kID", "lID"
  );

  // All rules that we want to ignore.
  private static final Set<String> IGNORE_RULES = new HashSet<>(Arrays.asList(
      "org.apache.calcite.rel.rules.custom.BestMatchNullifyPullUpRule\n"
  ));

  // The maximum number of queries to verify.
  private static final int MAX_QUERIES = 100_000;

  // The expected number of tables.
  private static final int NUM_TABLES = 5;

  // The default delimiter used in result output.
  private static final String PAIR_DELIMITER
      = "=============================================================";
  private static final String INTERNAL_DELIMITER
      = "-------------------------------------------------------------";
  private static final String LINE_DELIMITER = "\n";

  // The batch size used when printing progress bar.
  private static final int BATCH_SIZE = 10;

  // The description for output.
  private static final String WRONG_DESC = "The following 2 queries are not equivalent.";
  private static final String ERROR_DESC = "Meet exception when comparing the following 2 queries: %s.";

  // The names of the files to store logs.
  private static final Path OUT_PATH = Paths.get("out.log");
  private static final Path ERR_PATH = Paths.get("out.err.log");

  // Whether to wrap the input.
  private final boolean wrapInput;
  // Database connection.
  private final Connection connection;

  /**
   * Creates a new {@link ResultVerifier}.
   *
   * @param wrapInput is a flag on whether the input queries should be wrapped.
   * @param connection is the database connection.
   */
  public ResultVerifier(final boolean wrapInput, final Connection connection) {
    this.wrapInput = wrapInput;
    this.connection = connection;
  }

  /**
   * Verify queries from a given file.
   *
   * @param fileName is the name of the given file.
   * @throws IOException when there is any I/O error.
   */
  public void verify(final String fileName) throws IOException {
    // Reads the input.
    final Map<QueryPair, String> pairs = readInput(fileName);
    System.out.printf("We have discovered %d pairs of queries.\n", pairs.size());

    // Output stream to output log.
    final Writer outWriter = Files.newBufferedWriter(OUT_PATH);
    // Output stream to error log.
    final Writer errWriter = Files.newBufferedWriter(ERR_PATH);

    // Compares each pair of queries.
    final AtomicInteger count = new AtomicInteger(0);
    final AtomicInteger wrongCount = new AtomicInteger(0);
    final AtomicInteger errorCount = new AtomicInteger(0);

    pairs.entrySet().parallelStream().forEach(entry -> {
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
      if (!queryA.isEmpty() && !queryB.isEmpty()) {
        try {
          if (!compareQueryResult(connection, queryA, queryB)) {
            wrongCount.incrementAndGet();
            printQueryPair(outWriter, entry.getKey(), WRONG_DESC, entry.getValue());
          }
        } catch (SQLException e) {
          errorCount.incrementAndGet();

          final String desc = String.format(ERROR_DESC, e);
          printQueryPair(errWriter, entry.getKey(), desc, entry.getValue());
        }
      }

      // Prints the progress bar (if necessary).
      final int currentCount = count.incrementAndGet();
      if (currentCount % BATCH_SIZE == 0) {
        System.out.printf("Progress: %d out of %d (%d wrong & %d error).\n",
            currentCount, pairs.size(), wrongCount.get(), errorCount.get());
      }
    });

    // Closes the output streams.
    outWriter.flush();
    outWriter.close();
    errWriter.flush();
    errWriter.close();
  }

  /**
   * Reads input from a given file.
   *
   * @param fileName is the path to the given file.
   * @return all pairs of queries in that file.
   * @throws IOException when there is any I/O error.
   */
  private Map<QueryPair, String> readInput(String fileName) throws IOException {
    // Creates the reader.
    final FileReader fileReader = new FileReader(fileName);
    final BufferedReader reader = new BufferedReader(fileReader);

    // Reads line by line.
    Map<QueryPair, String> result = new HashMap<>();
    while (PAIR_DELIMITER.equals(reader.readLine())) {
      String origin = readUntil(reader, INTERNAL_DELIMITER);
      String first = readUntil(reader, INTERNAL_DELIMITER);
      String second = readUntil(reader, INTERNAL_DELIMITER);
      String description = readUntil(reader, PAIR_DELIMITER);

      if (!IGNORE_RULES.contains(description)) {
        QueryPair queryPair = new QueryPair(origin, first, second);
        result.put(queryPair, description);
      }

      if (result.size() >= MAX_QUERIES) {
        break;
      }
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
  private String readUntil(BufferedReader reader, String delimiter) throws IOException {
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
   * Wraps an input query to guarantee the ordering in its SELECT clause.
   *
   * @param input is the input query.
   * @return the wrapped query.
   */
  private String wrapQuery(String input) {
    String availableFields = FIELD_NAMES.stream()
        .filter(field -> input.contains(field) || input.contains("\"" + field.substring(0, 1) + "\""))
        .collect(Collectors.joining("\", \""));

    // Only proceeds if this is potentially a root node (i.e., contains all tables and
    // best-match operator).
    if (availableFields.length() != NUM_TABLES * 7 - 4 || !input.contains("PRECEDING")) {
      return "";
    }
    return String.format(WRAP_QUERY, availableFields, input);
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
  private boolean compareQueryResult(Connection connection, String queryA, String queryB) throws SQLException {
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
   * Prints out information about a pair of queries.
   *
   * @param writer is the given output stream.
   * @param pair is the pair of queries.
   * @param description is the description for this pair.
   * @param type is the type of the transformation.
   */
  private void printQueryPair(Writer writer, QueryPair pair, String description, String type) {
    final String str = PAIR_DELIMITER + "\n"
        + description
        + "\n\nOriginal query:\n\n"
        + pair.origin + "\n"
        + INTERNAL_DELIMITER + "\n"
        + "First query:\n\n"
        + pair.first + "\n"
        + INTERNAL_DELIMITER + "\n"
        + "Second query:\n\n"
        + pair.second + "\n"
        + INTERNAL_DELIMITER + "\n"
        + type + "\n"
        + PAIR_DELIMITER + "\n";

    // Prints the output.
    try {
      writer.write(str);
    } catch (IOException e) {
      System.err.println(str);
    }
  }
}
