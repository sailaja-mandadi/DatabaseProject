package MaxwellBase;

import Constants.Constants;

import static java.lang.System.out;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Locale;

public class Commands {

    /* This method determines what type of command the userCommand is and
     * calls the appropriate method to parse the userCommand String.
     */
    public static void parseUserCommand (String userCommand) {

        /* Clean up command string so that each token is separated by a single space */
        userCommand = userCommand.replaceAll("\n", " ");    // Remove newlines
        userCommand = userCommand.replaceAll("\r", " ");    // Remove carriage returns
        userCommand = userCommand.replaceAll(",", " , ");   // Tokenize commas
        userCommand = userCommand.replaceAll("\\(", " ( "); // Tokenize left parentheses
        userCommand = userCommand.replaceAll("\\)", " ) "); // Tokenize right parentheses
        userCommand = userCommand.replaceAll("( )+", " ");  // Reduce multiple spaces to a single space

        /* commandTokens is an array of Strings that contains one lexical token per array
         * element. The first token can be used to determine the type of command
         * The other tokens can be used to pass relevant parameters to each command-specific
         * method inside each case statemenut
         */
        ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));

        /*
         *  This switch handles a very small list of hard-coded commands from SQL syntax.
         *  You will want to rewrite this method to interpret more complex commands.
         */
        switch (commandTokens.get(0).toLowerCase()) {
            case "show":
                System.out.println("Case: SHOW");
                show(commandTokens);
                break;
            case "select":
                System.out.println("Case: SELECT");
                parseQuery(commandTokens);
                break;
            case "create":
                System.out.println("Case: CREATE");
                parseCreateTable(userCommand);
                break;
            case "insert":
                System.out.println("Case: INSERT");
                parseInsert(commandTokens);
                break;
            case "delete":
                System.out.println("Case: DELETE");
                parseDelete(commandTokens);
                break;
            case "update":
                System.out.println("Case: UPDATE");
                parseUpdate(commandTokens);
                break;
            case "drop":
                System.out.println("Case: DROP");
                dropTable(commandTokens);
                break;
            case "help":
                help();
                break;
            case "version":
                displayVersion();
                break;
            case "exit":
                Settings.setExit(true);
                break;
            case "quit":
                Settings.setExit(true);
                break;
            default:
                System.out.println("I didn't understand the command: \"" + userCommand + "\"");
                break;
        }
    }

    public static void displayVersion() {
        System.out.println("MaxwellBaseLite Version " + Settings.getVersion());
        System.out.println(Settings.getCopyright());
    }

    public static void parseCreateTable(String command) {
        /* TODO: Before attempting to create new table file, check if the table already exists */

        System.out.println("Stub: parseCreateTable method");
        System.out.println("Command: " + command);
        ArrayList<String> commandTokens = commandStringToTokenList(command);

        /* Extract the table name from the command string token list */
        String tableFileName = commandTokens.get(2) + ".tbl";

        /* YOUR CODE GOES HERE */

        /*  Code to create a .tbl file to contain table data */
        try {
            /*  Create RandomAccessFile tableFile in read-write mode.
             *  Note that this doesn't create the table file in the correct directory structure
             */

            /* Create a new table file whose initial size is one page (i.e. page size number of bytes) */
            RandomAccessFile tableFile = new RandomAccessFile("data/user_data/" + tableFileName, "rw");
            tableFile.setLength(Settings.getPageSize());

            /* Write page header with initial configuration */
            tableFile.seek(0);
            tableFile.writeInt(0x0D);       // Page type
            tableFile.seek(0x02);
            tableFile.writeShort(0x01FF);   // Offset beginning of cell content area
            tableFile.seek(0x06);
            tableFile.writeInt(0xFFFFFFFF); // Sibling page to the right
            tableFile.seek(0x0A);
            tableFile.writeInt(0xFFFFFFFF); // Parent page
        }
        catch(Exception e) {
            System.out.println(e);
        }

        /*  Code to insert an entry in the TABLES meta-data for this new table.
         *  i.e. New row in davisbase_tables if you're using that mechanism for meta-data.
         */

        /*  Code to insert entries in the COLUMNS meta data for each column in the new table.
         *  i.e. New rows in davisbase_columns if you're using that mechanism for meta-data.
         */
    }

    public static void show(ArrayList<String> commandTokens) {
        System.out.println("Command: " + tokensToCommandString(commandTokens));
        //System.out.println("Stub: This is the show method");
        ArrayList<String> selectQuery = new ArrayList<>();
        if (commandTokens.get(1).toLowerCase().equals("tables")) {
            selectQuery.add("SELECT");
            selectQuery.add("table_name"); //column-names column from
            selectQuery.add("FROM");
            selectQuery.add("maxwellbase_tables");
            parseQuery(selectQuery);
        }
        else{
            System.out.println("Command is incorrect.\nType \"help;\" to display supported commands.");
        }
        /* TODO: Your code goes here */
    }

    /**
     *  Stub method for executing queries
     */
    public static void parseQuery(ArrayList<String> commandTokens) {
        // Where to be handled later
        //System.out.println("Stub: This is the parseQuery method");
        System.out.println("Command: " + tokensToCommandString(commandTokens));
        boolean allColumns = false;
        String columnName = new String();
        String value = new String();
        String operator = new String();
        ArrayList<String> columns = new ArrayList<>();
        String tableName = new String();
        ArrayList<Record> result = new ArrayList<>();
        int queryLength = commandTokens.size();
        if(queryLength == 1) {
            System.out.println("Query is incorrect.\nType \"help;\" to display supported commands.");
            return;
        }
        int i = 1;
        if(commandTokens.get(i).equals("*")) {
            allColumns = true;
            i++;
        }
        else {
            while ( i < queryLength && !(commandTokens.get(i).toLowerCase().equals("from")) ) {
                if(!(commandTokens.get(i).toLowerCase().equals(",")))columns.add(commandTokens.get(i));
                i++;
            }
            if (i == queryLength) {
                System.out.println("Query is incorrect.\nType \"help;\" to display supported commands.");
                return;
            }
        }
        i++;
        if (i == queryLength) {
            System.out.println("Query is incorrect.\nType \"help;\" to display supported commands.");
            return;
        }

        tableName = commandTokens.get(i);
        Table table  = null;
//        try {
//            table = new Table(tableName);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
        //   System.out.println("test1:"+ tableName+" "+ allColumns+" "+columns);
        i++;
        if(queryLength == i) {
            try {
                //System.out.println("")
                result = table.search(null, null, null);
                // System.out.println("test2:"+ tableName+" "+ allColumns+" "+columns);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        else if(commandTokens.get(i).toLowerCase().equals("where")){
            System.out.println("i,test:"+ i );
            i++;
            if (i+3 == queryLength || i+4 == queryLength) {
                if(commandTokens.get(i).toLowerCase().equals("not")) {
                    columnName = commandTokens.get(i + 1);
                    value = commandTokens.get(i + 3);
                    operator = commandTokens.get(i + 2);
//                    if(table.getColumnType(columnName).toString().equals("DATE"))
//                        value = DataFunctions.toDate(value);


                    // System.out.println("test3:"+ columnName+" "+value+" "+operator);
                    switch (operator) {
                        case "=" -> {
                            try {
                                result = table.search(columnName,value,"<>");
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        case "<>" -> {
                            try {
                                result = table.search(columnName,value,"=");
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        case ">" -> {
                            try {
                                result = table.search(columnName,value,"<=");
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        case "<" -> {
                            try {
                                result = table.search(columnName,value,">=");
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        case ">=" -> {
                            try {
                                result = table.search(columnName,value,"<");
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        case "<=" -> {
                            try {
                                result = table.search(columnName,value,">");
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        default -> {
                            System.out.println("Operator is incorrect.\nType \"help;\" to display supported commands.");
                        }
                    }
                }
                else{
                    columnName = commandTokens.get(i);
                    operator =  commandTokens.get(i+1);
                    value =  commandTokens.get(i+2);
                    try {
                        // System.out.println("test:"+ columnName+" "+value+" "+operator);
                        result = table.search(columnName,value,operator);

                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            else{
                System.out.println("Query is incorrect.\nType \"help;\" to display supported commands.");
                return;
            }

            Commands.display(table,result,columns,allColumns);
        }
    }

    /*
     *  Stub method for inserting a new record into a table.
     */
    public static void parseInsert (ArrayList<String> commandTokens) {
        System.out.println("Command: " + tokensToCommandString(commandTokens));
        System.out.println("Stub: This is the insertRecord method");
        ArrayList<Constants.DataTypes> columnDatatype = new ArrayList<>();
        ArrayList<Object> values = new ArrayList<>();
        // How to get row ID ?
        // INSERT INTO Students ( name, ID) Values(sailaja, 4) ;
        // INSERT INTO Students (sailaja,4);
        // Adding column names to columns, values to values list
        /* TODO: Your code goes here */

    }

    public static void parseDelete(ArrayList<String> commandTokens) {
        System.out.println("Command: " + tokensToCommandString(commandTokens));
        System.out.println("Stub: This is the deleteRecord method");
        /* TODO: Your code goes here */
    }


    /**
     *  Stub method for dropping tables
     */
    public static void dropTable(ArrayList<String> commandTokens) {
        // removing record from catalogue
        System.out.println("Command: " + tokensToCommandString(commandTokens));
        System.out.println("Stub: This is the dropTable method.");
    }



    /**
     *  Stub method for updating records
     *  updateString is a String of the user input
     */
    public static void parseUpdate(ArrayList<String> commandTokens) {
        System.out.println("Command: " + tokensToCommandString(commandTokens));
        System.out.println("Stub: This is the parseUpdate method");
    }

    public static String tokensToCommandString (ArrayList<String> commandTokens) {
        String commandString = "";
        for(String token : commandTokens)
            commandString = commandString + token + " ";
        return commandString;
    }

    public static ArrayList<String> commandStringToTokenList (String command) {
        command.replace("\n", " ");
        command.replace("\r", " ");
        command.replace(",", " , ");
        command.replace("\\(", " ( ");
        command.replace("\\)", " ) ");
        ArrayList<String> tokenizedCommand = new ArrayList<String>(Arrays.asList(command.split(" ")));
        return tokenizedCommand;
    }

    /**
     *  Help: Display supported commands
     */
    public static void help() {
        out.println(Utils.printSeparator("*",80));
        out.println("SUPPORTED COMMANDS\n");
        out.println("All commands below are case insensitive\n");
        out.println("SHOW TABLES;");
        out.println("\tDisplay the names of all tables.\n");
        out.println("SELECT âŸ¨column_listâŸ© FROM table_name [WHERE condition];\n");
        out.println("\tDisplay table records whose optional condition");
        out.println("\tis <column_name> = <value>.\n");
        out.println("INSERT INTO (column1, column2, ...) table_name VALUES (value1, value2, ...);\n");
        out.println("\tInsert new record into the table.");
        out.println("UPDATE <table_name> SET <column_name> = <value> [WHERE <condition>];");
        out.println("\tModify records data whose optional <condition> is\n");
        out.println("DROP TABLE table_name;");
        out.println("\tRemove table data (i.e. all records) and its schema.\n");
        out.println("VERSION;");
        out.println("\tDisplay the program version.\n");
        out.println("HELP;");
        out.println("\tDisplay this help information.\n");
        out.println("EXIT;");
        out.println("\tExit the program.\n");
        out.println(Utils.printSeparator("*",80));
    }


    /**
     * function to display records returned in a search query as a Table in command line
     * @param table - name of table
     * @param data - list of records that are result of a search query
     * @param selColumns - columns to be displayed as per select query
     * @param allColumns - boolean to know if there is a "*" wildcard in select query
     */
    public static void display(Table table, ArrayList<Record> data, ArrayList<String> selColumns, boolean allColumns){
        ArrayList<Integer> columnNum = new ArrayList<>();
        ArrayList<Integer> colSize = new ArrayList<>();
        if (allColumns){
            for(int i = 0; i < table.columnNames.size();i++ )
                    columnNum.add(i);
        }
        else{
            for(String column : selColumns){
                for(int i = 0; i < table.columnNames.size();i++ )
                    if(table.columnNames.get(i).equals(column))
                        columnNum.add(i);
            }
        }

        Collections.sort(columnNum);

        for(Integer i : columnNum){
            int maxLength = table.columnNames.get(i).length();
            for(int iter = 0; iter < data.size();i++){
                int len = data.get(i).getValues().get(i).toString().trim().length();
                maxLength = len > maxLength ? len : maxLength;
            }
            colSize.add(maxLength);
        }
        int totalLength = (columnNum.size()-1)*3 + 4 ;
        for(Integer i : colSize)
            totalLength +=colSize.get(i);

        //print a line
        System.out.println(Utils.printSeparator("-",totalLength));
        //print column names
        String temp = "| ";
        for(Integer i : columnNum){
            temp +=" "+ String.format("%-"+colSize.get(i)+"s",table.columnNames.get(i)) + " |";
        }
        System.out.println(temp);
        //print a line
        System.out.println(Utils.printSeparator("-",totalLength));
        // print data
        for(int rec =0; rec<data.size();rec++){
            temp = "| ";
            for(Integer col : columnNum){
                temp +=" "+ String.format("%-"+colSize.get(col)+"s",data.get(rec).getValues().get(col).toString().trim()) + " |";
            }
            System.out.println(temp);
        }
        //print a line
        System.out.println(Utils.printSeparator("-",totalLength));

    }

}
