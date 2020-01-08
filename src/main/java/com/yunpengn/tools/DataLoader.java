package com.yunpengn.tools;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A class to load data into different tables.
 */
public class DataLoader {
  // All possible table names.
  private static final List<String> tableNames = List.of(
      "a", "b", "c", "d", "e"
  );

  // All query templates.
  private static final String TRUNCATE_QUERY = "TRUNCATE TABLE %s";
  private static final String INSERT_QUERY = "INSERT INTO \"%s\" (\"%s\") VALUES (%%d)";

  // The default range for input data.
  private static final int defaultLower = 0;
  private static final int defaultUpper = 100;

  // The database connection.
  private final Connection connection;
  // Whether to truncate tables before inserting new data.
  private final boolean truncateTable;
  // The number of tables to insert into.
  private final int numTables;

  public DataLoader(final Connection connection, final boolean truncateTable) {
    this(connection, truncateTable, tableNames.size());
  }

  public DataLoader(final Connection connection, final boolean truncateTable, final int numTables) {
    this.connection = connection;
    this.truncateTable = truncateTable;
    this.numTables = numTables;
  }

  public void load(final int numRows) throws SQLException {
    load(defaultLower, defaultUpper, numRows);
  }

  public void load(final int lower, final int upper, final int numRows) throws SQLException {
    // Truncate all tables if necessary.
    if (truncateTable) {
      truncateTables();
    }

    // Insert data into each table.
    for (int i = 0; i < numTables; i++) {
      final String tableName = tableNames.get(i);

      // Generates data.
      final List<Integer> numbers = IntStream.rangeClosed(lower, upper).boxed().collect(Collectors.toList());
      Collections.shuffle(numbers);
      final List<Integer> values = numbers.subList(0, numRows);

      // Fills in the current table.
      insertTable(tableName, values);
    }
  }

  /**
   * Truncates all tables.
   *
   * @throws SQLException when unable to execute any query.
   */
  private void truncateTables() throws SQLException {
    for (int i = 0; i < numTables; i++) {
      final String tableName = tableNames.get(i);
      final String query = String.format(TRUNCATE_QUERY, tableName);

      final Statement statement = connection.createStatement();
      statement.execute(query);
      statement.close();
    }
  }

  /**
   * Inserts data into a given table.
   *
   * @param tableName is the table's name.
   * @param values are all data to be inserted.
   * @throws SQLException when unable to execute any query.
   */
  private void insertTable(final String tableName, final List<Integer> values) throws SQLException {
    final String columnName = tableName + "ID";
    final String queryTemplate = String.format(INSERT_QUERY, tableName, columnName);

    // Inserts each value.
    for (Integer value: values) {
      final String query = String.format(queryTemplate, value);
      final Statement statement = connection.createStatement();
      statement.execute(query);
      statement.close();
    }
  }
}
