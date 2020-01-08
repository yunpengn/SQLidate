package com.yunpengn.tools;

import java.util.List;

/**
 * A class to load data into different tables.
 */
public class DataLoader {
  // All possible table names.
  private static final List<String> tableNames = List.of(
      "a", "b", "c", "d", "e"
  );

  // The default range for input data.
  private static final int defaultLower = 0;
  private static final int defaultUpper = 100;

  // Whether to truncate tables before inserting new data.
  private final boolean truncateTable;
  // The number of tables to insert into.
  private final int numTables;

  public DataLoader(final boolean truncateTable) {
    this(truncateTable, tableNames.size());
  }

  public DataLoader(final boolean truncateTable, final int numTables) {
    this.truncateTable = truncateTable;
    this.numTables = numTables;
  }

  public void load(final int numRows) {
    load(defaultLower, defaultUpper, numRows);
  }

  public void load(final int lower, final int upper, final int numRows) {

  }
}
