/**
 * Starter.java
 *
 * @author Santosh Kumar Nunna (sn7916@rit.edu)
 * This file is the main class file. The loader class receives the input arguments and calls the necessary functions
 * in order of required execution.
 */
public class Starter {
    public static void main(String[] args) {
        // Calling the createTheTables method
        Table.createTheTables(args);
        // Calling the DataLoader method
        Data.DataLoader(args);
    }
}
