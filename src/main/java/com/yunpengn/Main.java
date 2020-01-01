package com.yunpengn;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * The main driver class.
 */
public class Main {
  // The name for configuration file.
  private static final String PROPERTY_FILE_NAME = "config.properties";

  // The number of columns that is supposed to be in the result set.
  private static final int NUM_COLUMNS = 5;

  public static void main(String[] args) throws Exception {
    // Input validation.
    if (args.length == 0) {
      System.out.println("Usage: java -jar XXX.jar <input_file_path>");
    }
    String inputFile = args[0];

    // Creates the database connection.
    Connection connection = createConnection();

    // Reads the input and compares each pair of queries.
    List<Pair> pairs = readInput(inputFile);
    for (Pair pair: pairs) {
      if (!compareQueryResult(connection, pair.first, pair.second)) {
        System.err.println("==============================================================");
        System.err.println("The following 2 queries are not equivalent:\n");
        System.err.println("First query: ");
        System.err.println(pair.first);
        System.err.println("--------------------------------------------------------------");
        System.err.println("Second query: ");
        System.err.println(pair.second);
        System.err.println("==============================================================");
      }
    }

    // Closes the database connection.
    connection.close();
  }

  private static Connection createConnection() throws Exception {
    final Properties props = new Properties();
    props.load(new FileInputStream(PROPERTY_FILE_NAME));

    String dbName = props.getProperty("db");
    String url = "jdbc:postgresql://localhost/" + dbName;
    return DriverManager.getConnection(url, props);
  }

  private static boolean compareQueryResult(Connection connection, String queryA, String queryB) throws Exception {
    // Executes the first query.
    Statement statementA = connection.createStatement();
    ResultSet resultA = statementA.executeQuery(queryA);

    // Retrieves the rows in first result.
    List<List<Integer>> resultSetA = new ArrayList<>();
    while (resultA.next()) {
      List<Integer> row = new ArrayList<>(NUM_COLUMNS);
      for (int i = 0; i < NUM_COLUMNS; i++) {
        row.add(resultA.getInt(i));
      }
      resultSetA.add(row);
    }
    statementA.close();

    // Executes the second query.
    Statement statementB = connection.createStatement();
    ResultSet resultB = statementB.executeQuery(queryB);

    // Retrieves the rows in second result.
    List<List<Integer>> resultSetB = new ArrayList<>();
    while (resultB.next()) {
      List<Integer> row = new ArrayList<>(NUM_COLUMNS);
      for (int i = 0; i < NUM_COLUMNS; i++) {
        row.add(resultB.getInt(i));
      }
      resultSetB.add(row);
    }
    statementB.close();

    // Compares the two results.
    return resultSetA.equals(resultSetB);
  }

  private static List<Pair> readInput(String fileName) throws Exception {
    FileReader fileReader = new FileReader(fileName);
    BufferedReader reader = new BufferedReader(fileReader);
    return new ArrayList<>();
  }
}
