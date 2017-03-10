package com.cloudera.fce.exampleclients.common;

import java.sql.*;

public abstract class JdbcClient {

  protected Connection conn;

  public void openConnection(String connectionUrl) throws SQLException {
    conn = DriverManager.getConnection(connectionUrl);
  }

  public void closeConnection() throws SQLException {
    if (null != conn) {
      conn.close();
    }
  }

  /**
   * Simple method to run a single query via JDBC. Uses "default" database.
   * @param query
   * @throws SQLException
   */
  public void runQuery(String query) throws SQLException {
    runQuery(query, "default");
  }

  /**
   * Simple method to run a single query against a specific database
   * @param query
   * @param db
   * @throws SQLException
   */
  public void runQuery(String query, String db) throws SQLException {
    if (null == conn) {
      throw new IllegalStateException("No connection open");
    }
    Statement stmt = conn.createStatement();

    // Should use a prepared statement
    stmt.execute("use " + db);
    try {
      if (stmt.execute(query)) {
        ResultSet resultSet = stmt.getResultSet();
        ResultSetMetaData metadata = resultSet.getMetaData();
        int cols = metadata.getColumnCount();
        while (resultSet.next()) {
          for (int c = 1; c <= cols; ++c) {
            System.out.print(resultSet.getObject(c).toString());
            if (c != cols) System.out.print("\t");
            else System.out.println();
          }
        }
      } else {
        System.err.println("Query failed");
        System.err.println("Warnings: " + stmt.getWarnings());
      }
    } finally {
      stmt.close();
    }
  }

}
