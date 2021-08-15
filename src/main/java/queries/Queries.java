package queries;

import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * Runs queries against a back-end database
 */
public class Queries {
  // DB Connection
  private Connection conn;

  // Canned queries
  private static final String EXAMPLE_QUERY = "SELECT COUNT(*) AS count FROM Flights;";
  private static final String LONGER_EXAMPLE_QUERY = "SELECT w.day_of_week as day, COUNT(*) as count FROM Flights f, Weekdays w WHERE f.day_of_week_id = w.did GROUP BY w.did, w.day_of_week;";
  private static final String YOUR_QUERY = "SELECT C.name AS name, F.origin_city AS city, SUM(F.departure_delay) AS delay FROM FLIGHTS AS F, CARRIERS AS C WHERE F.carrier_id = C.cid GROUP BY F.origin_city, C.name, F.origin_city ORDER BY delay DESC;";
  

  public static void main(String[] args) throws IOException, SQLException {
	  Connection conn = openConnectionFromDbConn();
	  
	  int numFlights = getFlightCounts(conn);
	  System.out.println("Rows in Flights table: " + numFlights);
	  
	  printFlightsByDay(conn);

	  String name = fifth_slowest_airline(conn);

      System.out.println("Fifth slowest airline is: " + name);
	  
	  //always close the connection when you are done
	  conn.close();
  }

  /**
   * Return a connection by using dbconn.properties file
   *
   * @throws SQLException
   * @throws IOException
   */
  public static Connection openConnectionFromDbConn() throws SQLException, IOException {
    // Connect to the database with the provided connection configuration
    Properties configProps = new Properties();
    configProps.load(new FileInputStream("dbconn.properties"));
    String serverURL = configProps.getProperty("flightapp.server_url");
    String dbName = configProps.getProperty("flightapp.database_name");
    String adminName = configProps.getProperty("flightapp.username");
    String password = configProps.getProperty("flightapp.password");
    return openConnectionFromCredential(serverURL, dbName, adminName, password);
  }

  /**
   * Return a connecion by using the provided parameter.
   *
   * @param serverURL example: example.database.widows.net
   * @param dbName    database name
   * @param adminName username to login server
   * @param password  password to login server
   *
   * @throws SQLException
   */
  protected static Connection openConnectionFromCredential(String serverURL, String dbName,
      String adminName, String password) throws SQLException {
    String connectionUrl =
        String.format("jdbc:sqlserver://%s:1433;databaseName=%s;user=%s;password=%s", serverURL,
            dbName, adminName, password);
    Connection conn = DriverManager.getConnection(connectionUrl);

    // By default, automatically commit after each statement
    conn.setAutoCommit(true);

    // By default, set the transaction isolation level to serializable
    conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

    return conn;
  }

  /*
   * prepare an SQL statement
   */
  private static PreparedStatement prepareStatement(Connection conn, String query) throws SQLException {
    return conn.prepareStatement(query);
  }

  /*
   * Run the first example query and return the count value
   * from the only row it returns.
   */
  public static int getFlightCounts(Connection conn) {
    try {
      PreparedStatement statement = prepareStatement(conn, EXAMPLE_QUERY);
	  
	  ResultSet results = statement.executeQuery();
      
	  results.next();
      int count = results.getInt("count");

	  results.close();
	  
	  return count;
	  
    } catch(SQLException e) {
		
      System.out.println(e.getMessage());
	  return -1;
    }
  }
  
  /*
   * Run the second example query and loop through all rows
   * printing the day and count columns for each.
   */
  public static void printFlightsByDay(Connection conn) {
    try {
      PreparedStatement statement = prepareStatement(conn, LONGER_EXAMPLE_QUERY);
	  
	  ResultSet results = statement.executeQuery();
      
	  while (results.next()) {
		  String day = results.getString("day");
		  int count = results.getInt("count");
		  System.out.println("Flights on " + day + ": " + count);
	  }
	  
	  results.close();
	  
    } catch(SQLException e) {
      System.out.println(e.getMessage());
    }
  }
  
  public static String fifth_slowest_airline(Connection conn) {
      try {
          PreparedStatement statement = prepareStatement(conn, YOUR_QUERY);

          ResultSet results = statement.executeQuery();

          int count = 0;
          while (results.next()) {
              if (count == 4) {
                  String name = results.getString("name");
                  return name;
              }
              count++;
          }

          results.close();
          return "";

      } catch(SQLException e) {
          System.out.println(e.getMessage());
          return "no result";
      }

  }

}
