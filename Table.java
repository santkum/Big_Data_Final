/**
 * Table.java
 *
 * @author Santosh Kumar Nunna (sn7916@rit.edu)
 * This class implements the required methodology to create the tables as per the ER diagram. The necessary structure,
 * primary, foreign keys assignment to the attributes are all set in here.
 */
import java.sql.*;

public class Table {
    public static void createTheTables(String[] args){
        // Load the JDBC driver and formulate the database URL from the given input arguments.
        final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
        final String DB_URL = args[0] + "&useUnicode=true&characterEncoding=UTF-8&user=" + args[1] + "&password=" + args[2];

        // Initialize the connection and statement objects
        Connection conn = null;
        Statement stmt = null;
        try {
            // Registering the jdbc driver
            Class.forName("com.mysql.jdbc.Driver");

            // Initiating the connection the database with the supplied URL
            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(DB_URL);

            // Creating input statement
            stmt = conn.createStatement();
            // SQL query to create the Genre table
            String sql = "CREATE TABLE Genre " +
                    "(Genre_ID INTEGER not NULL, " +
                    " Genre_Name VARCHAR(255), " +
                    " PRIMARY KEY ( Genre_ID ))";
            // Executing the sql query and closing the statement object
            stmt.executeUpdate(sql);
            stmt.close();

            // SQL query to create the Publisher table
            stmt = conn.createStatement();
            sql = "CREATE TABLE Publisher " +
                    "(Publisher_ID INTEGER not NULL, " +
                    " Publisher_Name VARCHAR(255), " +
                    " PRIMARY KEY ( Publisher_ID ))";
            // Executing the sql query and closing the statement object
            stmt.executeUpdate(sql);
            stmt.close();

            // SQL query to create the Platform table
            stmt = conn.createStatement();
            sql = "CREATE TABLE Platform " +
                    "(Platform_ID INTEGER not NULL, " +
                    " Platform_Name VARCHAR(255), " +
                    " PRIMARY KEY ( Platform_ID ))";
            // Executing the sql query and closing the statement object
            stmt.executeUpdate(sql);
            stmt.close();

            // SQL query to create the Sales table
            stmt = conn.createStatement();
            sql = "CREATE TABLE Sales " +
                    "(Sales_ID INTEGER not NULL, " +
                    " NA FLOAT, " +
                    " EU FLOAT, " +
                    " JP FLOAT, " +
                    " Others FLOAT, " +
                    " PRIMARY KEY (Sales_ID))";
            // Executing the sql query and closing the statement object
            stmt.executeUpdate(sql);
            stmt.close();

            // SQL query to create the Game table
            stmt = conn.createStatement();
            sql = "CREATE TABLE Game " +
                    "(Game_ID INTEGER not NULL, " +
                    " Game_Name VARCHAR(255), " +
                    " Platform INTEGER , " +
                    " Genre INTEGER, " +
                    " Publisher INTEGER, " +
                    " Year_Of_Release INTEGER, " +
                    " PRIMARY KEY (Game_ID)," +
                    " FOREIGN KEY (Platform) references Platform(Platform_ID), " +
                    " FOREIGN KEY (Genre) references Genre(Genre_ID), " +
                    " FOREIGN KEY (Publisher) references Publisher(Publisher_ID), " +
                    " FOREIGN KEY (Game_ID) references Sales(Sales_ID))";
            // Executing the sql query and closing the statement object
            stmt.executeUpdate(sql);
            stmt.close();

            System.out.println("Tables creation complete!!");
        }catch (Exception se) {
            se.printStackTrace();
        } finally {
            // Closing all the statement objects if they are still active
            try {
                if (stmt != null)
                    stmt.close();
                if (conn != null)
                    conn.close();
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }

        }
}
