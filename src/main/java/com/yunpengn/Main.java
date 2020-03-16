package com.yunpengn;

import com.yunpengn.tools.DataBigGenerator;
import com.yunpengn.tools.DataGenerator;
import com.yunpengn.tools.DataLoader;
import com.yunpengn.tools.ResultVerifier;
import com.yunpengn.tools.StatsChecker;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.ZonedDateTime;
import java.util.Properties;

/**
 * The main driver class.
 */
public class Main {
  // The name for configuration file.
  private static final String PROPERTY_FILE_NAME = "config.properties";

  /**
   * The main function.
   *
   * @param args are the CLI arguments.
   */
  public static void main(String[] args) throws Exception {
    // Input validation.
    if (args.length == 0) {
      System.err.println("Usage: java -jar XXX.jar <command>");
      return;
    }

    // Switch functionality based on first argument.
    System.out.println("Program started at " + ZonedDateTime.now() + ".\n");
    switch (args[0]) {
    case "check":
      checkQueryResult(args);
      break;
    case "generate":
      generate(args);
      break;
    case "big-generate":
      bigGenerate(args);
      break;
    case "load":
      loadData(args);
      break;
    case "stats":
      checkStats(args);
      break;
    default:
      System.err.println("Invalid command.");
    }
    System.out.println("\nProgram finished at " + ZonedDateTime.now() + ".\n");
  }

  /**
   * Checks query results.
   *
   * @param args are the CLI arguments.
   * @throws Exception when there is any error.
   */
  private static void checkQueryResult(String[] args) throws Exception {
    // Input validation.
    if (args.length == 1) {
      System.err.println("Usage: java -jar XXX.jar check <input_file_name> [Y/N]");
      return;
    }
    final String inputFile = args[1];

    // Whether to enable query wrapper.
    boolean wrapInput = true;
    if (args.length > 2) {
      switch (args[2].toLowerCase()) {
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
        System.err.println("Invalid argument: " + args[2]);
        return;
      }
    }

    // Creates the database connection & compares the queries.
    final Connection connection = createConnection();
    final ResultVerifier verifier = new ResultVerifier(wrapInput, connection);
    verifier.verify(inputFile);

    // Closes the database connection.
    connection.close();
  }

  private static void generate(String[] args) throws Exception {
    final DataGenerator generator = new DataGenerator();
    generator.run();
  }

  private static void bigGenerate(String[] args) throws Exception {
    final DataBigGenerator generator = new DataBigGenerator();
    generator.run();
  }

  private static void checkStats(String[] args) throws Exception {
    final StatsChecker checker = new StatsChecker();
    checker.run(args[1]);
  }

  /**
   * Loads data into database.
   *
   * @param args are the CLI arguments.
   */
  private static void loadData(String[] args) throws Exception {
    // Input validation.
    if (args.length == 1) {
      System.err.println("Usage: java -jar XXX.jar load <num_of_rows> [num_of_tables]");
      return;
    }
    final int numRows = Integer.parseInt(args[1]);

    // Number of tables.
    final Connection connection = createConnection();
    final boolean truncateTable = true;
    DataLoader loader = new DataLoader(connection, truncateTable);
    if (args.length > 2) {
      final int numTables = Integer.parseInt(args[2]);
      loader = new DataLoader(connection, truncateTable, numTables);
    }

    // Loads data.
    loader.load(numRows);

    // Closes the database connection.
    connection.close();
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
}
