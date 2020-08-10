/**
 * Data.java
 *
 * @author Santosh Kumar Nunna (sn7916@rit.edu)
 * This class contains the necessary methodology to load the data from the input .csv files to the tables created in the
 * schema. We perform the data cleaning before inserting into the tables. Here we remove the global sales information
 * as it is an extra attribute that is a sum of all the sales table. It is just an additional data.
 */
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.HashMap;
import java.util.Scanner;

public class Data {
    static HashMap<String, Integer> Platforms = new HashMap<>();
    static HashMap<String, Integer> Genres = new HashMap<>();
    static HashMap<String, Integer> Publishers = new HashMap<>();

    public static void DataLoader(String[] args){
        // Initializing the JDBC driver
        final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
        // Formulate the Database URL using the given arguments
        final String DB_URL = args[0] + "&useUnicode=true&characterEncoding=UTF-8&user=" + args[1] +
                "&password=" + args[2];
        // Initializing the connections objects and statement variables
        Connection conn = null;
        PreparedStatement stmtGame = null;
        PreparedStatement stmtPlatform = null;
        PreparedStatement stmtGenre = null;
        PreparedStatement stmtPublisher = null;
        PreparedStatement stmtSales = null;
        String[] splitArray = new String[0];
        String currentLine = null;
        // Reading filepath and initializing connection to the database using the URL and arguments.
        String filepath = args[3];
        try (InputStream gzipStream = new FileInputStream(filepath);
             Scanner sc = new Scanner(gzipStream, "UTF-8")
        ){
            // Connecting to database
            Class.forName("com.mysql.jdbc.Driver");
            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(DB_URL);

            // Initialize the commit object and set autocommit to False, we will manually commit data in chunks
            conn.setAutoCommit(false);
            sc.nextLine();// First Line

            // Insert statement creation with values to be executed for each table depending on the available data
            // and attributes.
            String insertStatementGame = "INSERT INTO Game(Game_ID, Game_Name, Platform, Genre, Publisher, Year_Of_Release) "
                    + "VALUES(?, ?, ?, ?, ?, ?)";
            String insertStatementPlatform = "INSERT INTO Platform(Platform_ID, Platform_Name) "
                    + "VALUES(?, ?)";
            String insertStatementGenre = "INSERT INTO Genre(Genre_ID, Genre_Name) "
                    + "VALUES(?, ?)";
            String insertStatementPublisher = "INSERT INTO Publisher(Publisher_ID, Publisher_Name) "
                    + "VALUES(?, ?)";
            String insertStatementSales = "INSERT INTO Sales(Sales_ID, NA, EU, JP, Others) "
                    + "VALUES(?, ?, ?, ?, ?)";
            // Initializing the prepare statements.
            stmtGame = conn.prepareStatement(insertStatementGame);
            stmtPlatform = conn.prepareStatement(insertStatementPlatform);
            stmtGenre = conn.prepareStatement(insertStatementGenre);
            stmtPublisher = conn.prepareStatement(insertStatementPublisher);
            stmtSales = conn.prepareStatement(insertStatementSales);
            int counter = 0;
            int platform_counter = 1;
            int publisher_counter = 1;
            int genre_counter = 1;

            while(sc.hasNextLine()) {
                currentLine = sc.nextLine();
                splitArray = currentLine.split(",");
                if (splitArray.length != 11){
                    continue;
                }
                counter++;
                // Splitting the input data row and assigning it to the relevant attributes.
                if (!Platforms.containsKey(splitArray[2])) {
                    Platforms.put(splitArray[2], platform_counter);
                    // Platform table entry
                    stmtPlatform.setInt(1, platform_counter);
                    stmtPlatform.setString(2, splitArray[2]);
                    stmtPlatform.addBatch();
                    platform_counter++;
                }
                if (!Genres.containsKey(splitArray[4])) {
                    Genres.put(splitArray[4], genre_counter);
                    // Genre table entry
                    stmtGenre.setInt(1, genre_counter);
                    stmtGenre.setString(2, splitArray[4]);
                    stmtGenre.addBatch();
                    genre_counter++;
                }
                if (!Publishers.containsKey(splitArray[5])) {
                    Publishers.put(splitArray[5], publisher_counter);
                    // Publisher table entry
                    stmtPublisher.setInt(1, publisher_counter);
                    stmtPublisher.setString(2, splitArray[5]);
                    stmtPublisher.addBatch();
                    publisher_counter++;
                }


                // Game table data
                stmtGame.setInt(1, Integer.parseInt(splitArray[0]));
                stmtGame.setString(2, splitArray[1]);
                stmtGame.setInt(3, Platforms.get(splitArray[2]));
                stmtGame.setInt(4, Genres.get(splitArray[4]));
                stmtGame.setInt(5, Publishers.get(splitArray[5]));
                // Checking for invalid string availability and replacing it with default value
                if (splitArray[3].equals("N/A")){
                    stmtGame.setInt(6, 2000);
                }else{
                    stmtGame.setInt(6, Integer.parseInt(splitArray[3]));
                }
                stmtGame.addBatch();

                // Sales table data
                stmtSales.setInt(1, Integer.parseInt(splitArray[0]));
                stmtSales.setFloat(2, Float.parseFloat(splitArray[6]));
                stmtSales.setFloat(3, Float.parseFloat(splitArray[7]));
                stmtSales.setFloat(4, Float.parseFloat(splitArray[8]));
                stmtSales.setFloat(5, Float.parseFloat(splitArray[9]));
                stmtSales.addBatch();

                if (counter % 1000 == 0){
                    stmtPlatform.executeBatch();
                    stmtGenre.executeBatch();
                    stmtPublisher.executeBatch();
                    stmtSales.executeBatch();
                    stmtGame.executeBatch();
                    conn.commit();
                }

            }

            // Final execution of batch statements
            stmtPlatform.executeBatch();
            stmtGenre.executeBatch();
            stmtPublisher.executeBatch();
            stmtSales.executeBatch();
            stmtGame.executeBatch();
            // Final commit
            conn.commit();
            // Closing the connection
            conn.close();
            System.out.println("Data insertion is complete!!");
        }catch (Exception se) {
            System.out.println(currentLine);
            se.printStackTrace();
            // Close the statement objects or flush them if they are not emptied to prevent unnecessary duplicate and
            // dangling pointers
        } finally {
            try {
                if (stmtGame != null)
                    stmtGame.close();
                if (stmtGenre != null)
                    stmtGenre.close();
                if (stmtPlatform != null)
                    stmtPlatform.close();
                if (stmtPublisher != null)
                    stmtPublisher.close();
                if (stmtSales != null)
                    stmtSales.close();
                if (conn != null)
                    conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
