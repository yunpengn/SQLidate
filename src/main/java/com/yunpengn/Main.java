package com.yunpengn;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * The main driver class.
 */
public class Main {
  private static final String PROPERTY_FILE_NAME = "config.properties";

  public static void main(String[] args) throws Exception {
    Connection connection = createConnection();
  }

  private static Connection createConnection() throws Exception {
    final Properties props = new Properties();
    props.load(new FileInputStream(PROPERTY_FILE_NAME));

    String dbName = props.getProperty("db");
    String url = "jdbc:postgresql://localhost/" + dbName;
    return DriverManager.getConnection(url, props);
  }
}
