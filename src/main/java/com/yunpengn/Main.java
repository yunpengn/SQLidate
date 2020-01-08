package com.yunpengn;

import com.yunpengn.tools.ResultVerifier;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
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

    // Creates the database connection & compares the queries.
    final Connection connection = createConnection();
    final ResultVerifier verifier = new ResultVerifier(wrapInput, connection);
    verifier.verify(inputFile);

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
