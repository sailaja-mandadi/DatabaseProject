package MaxwellBase;//import java.io.File;
//import java.io.FileReader;
import java.io.File;
import java.util.Scanner;

/**
 *  @author Team Maxwell
 *  @version 0.0
 *  <b>
 *  <p>This is an example of how to create an interactive prompt</p>
 *  <p>There is also some guidance to get started with read/write of
 *     binary data files using the RandomAccessFile class from the
 *     Java Standard Library.</p>
 *  </b>
 *
 */
public class MaxwellBase {

    /*
     *  The Scanner class is used to collect user commands from the prompt
     *  There are many ways to do this. This is just one.
     *
     *  Each time the semicolon (;) delimiter is entered, the userCommand
     *  String is re-populated.
     */
    static Scanner scanner = new Scanner(System.in).useDelimiter(";");

    /** ***********************************************************************
     *  Main method
     */
    public static void main(String[] args) {

        /* Initializing */
        File dataDirectory = new File("data");
        /* Checking if data directory exit, if not create new directory of data storing Tables and Columns information
        * If any of the tables doesn't exist it will reinitialize both the tables. Scraping the current Columns and Tables table*/
        if (!dataDirectory.exists()){
            System.out.println("The data directory doesn't exit. Initializing...");
            testInitializing.initialize(dataDirectory);
        }
        else{
            testInitializing.initializeUserCatalogDirectory();
        }

        /* Display the welcome screen */
        Utils.splashScreen();

        /* Variable to hold user input from the prompt */
        String userCommand = "";

        while(!Settings.isExit()) {
            System.out.print(Settings.getPrompt());
            /* Strip newlines and carriage returns */
            userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim();
            Commands.parseUserCommand(userCommand);
        }
        System.out.println("Exiting...");
    }
}