package MaxwellBase;

import Constants.Constants;

import static java.lang.System.out;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Arrays;

public class Commands {

    /* This method determines what type of command the userCommand is and
     * calls the appropriate method to parse the userCommand String.
     */
    public static void parseUserCommand (String userCommand) throws IOException {

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

    public static void parseCreateTable(String command) throws IOException {
        /* TODO: Before attempting to create new table file, check if the table already exists */

        System.out.println("Stub: parseCreateTable method");
        System.out.println("Command: " + command);
        ArrayList<String> commandTokens = commandStringToTokenList(command);
        ArrayList<Record> result = new ArrayList<>();
        ArrayList<String> columnNames = new ArrayList<>();
        ArrayList<Constants.DataTypes> columnTypes = new ArrayList<>();
        ArrayList<Boolean> primaryKey = new ArrayList<>();
        ArrayList<Boolean> unique = new ArrayList<>();
        ArrayList<Boolean> isNull = new ArrayList<>();

        /* Extract the table name from the command string token list */
        String tableFileName = commandTokens.get(2);
        //ArrayList<String> selectQuery = new ArrayList<>();
        Table metatable = new Table("maxwellbase_tables");
        Table metacolumns = new Table("maxwellbase_columns");
        result = metatable.search("table_name", tableFileName, "=");
        if(result.get(0).getValues().get(0).toString().toLowerCase().equals(tableFileName.toLowerCase())){
            System.out.println("Table already exists!");

        }
        else{
            //Parsing the query
            int iter = 3;
            if(!commandTokens.get(iter++).equals("(")) {
                System.out.println("Command is incorrect.\nType \"help;\" to display supported commands.");
                return;
            }
            else {
                while(!commandTokens.get(iter).equals(")")){
                    ArrayList<String> temp = new ArrayList<>();
                    while(!commandTokens.get(iter).equals(",")){
                        temp.add(commandTokens.get(iter));iter++;
                    }
                    if(temp.size() < 2)
                        {System.out.println("Command is incorrect.\nType \"help;\" to display supported commands.");
                        return;}
                    else{
                        boolean pri = false; boolean uni = false; boolean nullable = true;
                        columnNames.add(temp.get(0));
                        columnTypes.add(datatypeOfStr(temp.get(1)));
                        int it = 2;
                        while(it < temp.size()){
                            if(temp.get(it).equals("PRIMARY KEY"))
                                { pri = true;  nullable = false;}
                            if(temp.get(it).equals("UNIQUE"))
                                uni = true;
                            if(temp.get(it).equals("NOT NULL"))
                                nullable = false;
                        }
                        primaryKey.add(pri);
                        unique.add(uni);
                        isNull.add(nullable);
                    }
                    iter++;
                }
                /*  Code to create a .tbl file to contain table data */
                Table newTable = new Table(tableFileName,columnNames,columnTypes,isNull,true);

                /*  Code to insert an entry in the TABLES meta-data for this new table.
                 *  i.e. New row in davisbase_tables if you're using that mechanism for meta-data.
                 */
                metatable.insert(new ArrayList<Object>(Arrays.asList(tableFileName)));

                /*  Code to insert entries in the COLUMNS meta data for each column in the new table.
                 *  i.e. New rows in davisbase_columns if you're using that mechanism for meta-data.
                 */
                for(int i =0; i< columnTypes.size();i++){
                    String isNullable = new String();
                    String columnKey = new String();
                    if(primaryKey.get(i)) columnKey = "PRI";
                    else if (unique.get(i)) columnKey = "UNI";
                    else columnKey = "NULL";

                    if(isNull.get(i))
                        isNullable = "YES";
                    else
                        isNullable = "NO";
                    metacolumns.insert(new ArrayList<Object>(Arrays.asList(tableFileName,columnNames.get(i),
                            columnTypes.get(i).toString(), i+1,
                            isNullable,columnKey)));
                }


            }
        }
    }

    public static void show(ArrayList<String> commandTokens) throws IOException {
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

    }

    /**
     *  Stub method for executing queries
     */
    public static void parseQuery(ArrayList<String> commandTokens) throws IOException {
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
       try {
            table = new Table(tableName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
                    // System.out.println("test3:"+ columnName+" "+value+" "+operator);
                    switch (operator) {
                        case "=" -> {operator = "<>";}
                        case "<>" -> {operator = "=";}
                        case ">" -> { operator = "<=";}
                        case "<" -> {operator = ">=";}
                        case ">=" -> {operator = "<";}
                        case "<=" -> {operator = ">";}
                        default -> {
                            System.out.println("Operator is incorrect.\nType \"help;\" to display supported commands.");
                        }
                    }
                    Constants.DataTypes type = table.getColumnType(columnName);
                    switch(type){
                        // YEAR values format : String YYYY
                        case YEAR -> {
                            value = DataFunctions.toDbYear(value);
                        }
                        // TIME values format : hh:mm:ss
                        case TIME -> {
                            value = DataFunctions.toDbTime(value);
                        }
                        // DATETIME values format : YYYY-MM-DD_hh:mm:ss
                        case DATETIME -> {
                            value = DataFunctions.toDbDateTime(value);
                        }
                        //DATE values format : YYYY-MM-DD
                        case DATE-> {
                            value = DataFunctions.toDbDateTime(value+"_00:00:00");
                        }
                        default -> {
                        }
                    }
                    result = table.search(columnName,value,operator);

                }
                else{
                    columnName = commandTokens.get(i);
                    operator =  commandTokens.get(i+1);
                    value =  commandTokens.get(i+2);
                    Constants.DataTypes type = table.getColumnType(columnName);
                    switch(type){
                        // YEAR values format : String YYYY
                        case YEAR -> {
                            value = DataFunctions.toDbYear(value);
                        }
                        // TIME values format : hh:mm:ss
                        case TIME -> {
                            value = DataFunctions.toDbTime(value);
                        }
                        // DATETIME values format : YYYY-MM-DD_hh:mm:ss
                        case DATETIME -> {
                            value = DataFunctions.toDbDateTime(value);
                        }
                        //DATE values format : YYYY-MM-DD
                        case DATE-> {
                            value = DataFunctions.toDbDateTime(value+"_00:00:00");
                        }
                        default -> {
                        }
                    }
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

    //public static ArrayList<String> condition(String a,Str)
    public static void parseInsert (ArrayList<String> commandTokens) throws IOException {
        out.println("Command: " + tokensToCommandString(commandTokens));
        out.println("Stub: This is the insertRecord method");
        ArrayList<Constants.DataTypes> columnDatatype = new ArrayList<>();

        if (commandTokens.size() < 5){
            out.println("Command is incorrect.\nType \"help;\" to display supported commands.");
            return;
        }

        if (!commandTokens.get(1).toLowerCase().equals("into")){
            out.println("Command is incorrect.\nType \"help;\" to display supported commands.");
            return;
        }
        else{
            String tableFileName = commandTokens.get(2);
            Table table = new Table(tableFileName);
            String[] values = new String[table.columnNames.size()];
            String[][] temp = new String[table.columnNames.size()][table.columnNames.size()];
            if (!commandTokens.get(3).equals("(") || !commandTokens.get(3).toLowerCase().equals("values")) {
                out.println("Command is incorrect.\nType \"help;\" to display supported commands.");
                return;
            }

            if(commandTokens.get(3).equals("(")){
                int iter = 4; int cptr = 0;
                while(!commandTokens.get(iter).equals(")")){
                    if(!commandTokens.get(iter).equals(",")){
                        temp[cptr++][0] = commandTokens.get(iter);
                        iter++;
                    }
                }
                iter++;
                if (!commandTokens.get(iter).toLowerCase().equals("values") ||
                        !commandTokens.get(iter+1).equals("(")) {
                    out.println("Command is incorrect.\nType \"help;\" to display supported commands.");
                    return;
                }
                else{
                    iter+=2;
                    while(!commandTokens.get(iter).equals(")")){
                        if(!commandTokens.get(iter).equals(",")){
                            temp[cptr++][1] = commandTokens.get(iter);
                            iter++;
                        }
                    }
                }

            }
            else if(commandTokens.get(3).toLowerCase().equals("values")){
                int iter = 4, vptr = 0;
                if (!commandTokens.get(iter).equals("(")) {
                    out.println("Command is incorrect.\nType \"help;\" to display supported commands.");
                    return;
                }
                else{
                    iter++;
                    while(!commandTokens.get(iter).equals(")")){
                        if(!commandTokens.get(iter).equals(",")){
                            temp[vptr][0]=table.columnNames.get(vptr);
                            temp[vptr++][1] = commandTokens.get(iter);
                            iter++;
                        }
                    }
                }
            }
            // create an array of values at appropriate positions
            for(int i = 0; i < temp.length;i++){
                int j = table.columnNames.indexOf(temp[i][0]);
                        values[j]=temp[i][1];
            }
            // for each null value , check if it can be nullable
            int flag = 0;
            for( flag = 0; flag < values.length;flag++){
                if(values[flag].equals(null)){
                    if (table.colIsNullable.get(flag)){
                        continue;
                    }
                    else{
                        out.println(table.columnNames.get(flag)+"can not be NULL!");
                        return;
                    }
                }
            }
            // if all values are checked, call insert
            if(flag == table.columnNames.size()){
                ArrayList<Object> insertvalues = new ArrayList<>();
                for( int i = 0; i < values.length;i++){
                    if(!values[i].equals(null)){
                        Constants.DataTypes type = table.columnTypes.get(i);
                        switch(type){
                            // YEAR values format : String YYYY
                            case YEAR -> {
                                String s = DataFunctions.toDbYear(values[i]);
                                insertvalues.add(DataFunctions.parseString(type,s));
                            }
                            // TIME values format : hh:mm:ss
                            case TIME -> {
                                String s = DataFunctions.toDbTime(values[i]);
                                insertvalues.add(DataFunctions.parseString(type,s));
                            }
                            // DATETIME values format : YYYY-MM-DD_hh:mm:ss
                            case DATETIME -> {
                                String s = DataFunctions.toDbDateTime(values[i]);
                                insertvalues.add(DataFunctions.parseString(type,s));
                            }
                            //DATE values format : YYYY-MM-DD
                            case DATE-> {
                                String s = DataFunctions.toDbDateTime(values[i]+"_00:00:00");
                                insertvalues.add(DataFunctions.parseString(type,s));
                            }
                            default -> {
                                insertvalues.add(DataFunctions.parseString(type,values[i]));
                            }
                        }
                        }
                    else
                       insertvalues.add(null);
                }
                table.insert(insertvalues);
            }
        }
    }

    public static void parseDelete(ArrayList<String> commandTokens) throws IOException {
        System.out.println("Command: " + tokensToCommandString(commandTokens));
        System.out.println("Stub: This is the deleteRecord method");

        String columnName = new String();
        String value = new String();
        String operator = new String();

        if(commandTokens.get(0).toLowerCase().equals("delete") && commandTokens.get(1).toLowerCase().equals("from") )
        {
            Table table = new Table(commandTokens.get(2));
            int querylength = commandTokens.size();
            if(querylength>3)
            {
                if (!commandTokens.get(3).toLowerCase().equals("where"))
                {
                    out.println("Command is InValid");
                }
                else
                {
                    if (querylength==7 || querylength==8) {
                        if(commandTokens.get(4).toLowerCase().equals("not")) {
                            columnName = commandTokens.get(5);
                            value = commandTokens.get(7);
                            operator = commandTokens.get(6);
                                switch(operator){
                                    case "=" -> {operator = "<>";}
                                    case "<>" -> {operator = "=";}
                                    case ">" -> {operator = "<=";}
                                    case "<" -> {operator = ">=";}
                                    case ">=" -> {operator = "<";}
                                    case "<=" -> {operator = ">";}
                                    default -> {
                                        System.out.println("Operator is incorrect.\nType \"help;\" to display supported commands.");
                                    }
                                }
                            Constants.DataTypes type = table.getColumnType(columnName);
                            switch(type){
                                // YEAR values format : String YYYY
                                case YEAR -> {
                                    value = DataFunctions.toDbYear(value);
                                }
                                // TIME values format : hh:mm:ss
                                case TIME -> {
                                    value = DataFunctions.toDbTime(value);
                                }
                                // DATETIME values format : YYYY-MM-DD_hh:mm:ss
                                case DATETIME -> {
                                    value = DataFunctions.toDbDateTime(value);
                                }
                                //DATE values format : YYYY-MM-DD
                                case DATE-> {
                                    value = DataFunctions.toDbDateTime(value+"_00:00:00");
                                }
                                default -> {
                                }
                            }
                            if(table.delete(columnName,value,operator))
                                System.out.print("delete is successful!");
                            else
                                System.out.print("delete failed!");

                        }
                        else{
                            columnName = commandTokens.get(4);
                            operator =  commandTokens.get(5);
                            value =  commandTokens.get(6);
                            Constants.DataTypes type = table.getColumnType(columnName);
                            switch(type){
                                // YEAR values format : String YYYY
                                case YEAR -> {
                                    value = DataFunctions.toDbYear(value);
                                }
                                // TIME values format : hh:mm:ss
                                case TIME -> {
                                    value = DataFunctions.toDbTime(value);
                                }
                                // DATETIME values format : YYYY-MM-DD_hh:mm:ss
                                case DATETIME -> {
                                    value = DataFunctions.toDbDateTime(value);
                                }
                                //DATE values format : YYYY-MM-DD
                                case DATE-> {
                                    value = DataFunctions.toDbDateTime(value+"_00:00:00");
                                }
                                default -> {
                                }
                            }
                            try {
                                // System.out.println("test:"+ columnName+" "+value+" "+operator);
                                if(table.delete(columnName,value,operator))
                                    System.out.print("delete is successful!");
                                else
                                    System.out.print("delete failed!");

                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                    else{
                        System.out.println("Query is incorrect.\nType \"help;\" to display supported commands.");
                        return;
                    }

                }
            }
            else
            {
                if(table.delete(null,null,null))
                    System.out.print("delete is successful!");
                else
                    System.out.print("delete failed!");
            }
        }
        else
        {
            out.println("Command is Invalid");
        }



    }


    /**
     *  Stub method for dropping tables
     */
    public static void dropTable(ArrayList<String> commandTokens) throws IOException {
        // removing record from catalogue

        System.out.println("Command: " + tokensToCommandString(commandTokens));
        System.out.println("Stub: This is the dropTable method.");
        if(commandTokens.get(1).toLowerCase().equals("table"))
        {
            Table table = new Table(commandTokens.get(2));
            table.dropTable();
        }
        else
        {
            out.println("Command is Invalid");
        }


    }



    /**
     *  Stub method for updating records
     *  updateString is a String of the user input
     */
    public static void parseUpdate(ArrayList<String> commandTokens) throws IOException {
        System.out.println("Command: " + tokensToCommandString(commandTokens));
        System.out.println("Stub: This is the parseUpdate method");
        String columnName = new String();
        String value = new String();
        String operator = new String();
        String updateCol = new String();
        String updateVal = new String();
        if(!commandTokens.get(0).toLowerCase().equals("update") || !commandTokens.get(2).toLowerCase().equals("set"))
        {
            out.println("Command is InValid");
        }
        else
        {
            Table table = new Table(commandTokens.get(1));
            updateCol = commandTokens.get(3);
            updateVal = commandTokens.get(5);

            int querylength = commandTokens.size();
            if(querylength>6) {
                if (!commandTokens.get(6).toLowerCase().equals("where")) {
                    out.println("Command is InValid");
                } else {
                    if (querylength == 10 || querylength == 11) {
                        if (commandTokens.get(7).toLowerCase().equals("not")) {
                            columnName = commandTokens.get(8);
                            value = commandTokens.get(10);
                            operator = commandTokens.get(9);
                            switch (operator) {
                                case "=" -> {
                                    operator = "<>";
                                }
                                case "<>" -> {
                                    operator = "=";
                                }
                                case ">" -> {
                                    operator = "<=";
                                }
                                case "<" -> {
                                    operator = ">=";
                                }
                                case ">=" -> {
                                    operator = "<";
                                }
                                case "<=" -> {
                                    operator = ">";
                                }
                                default -> {
                                    System.out.println("Operator is incorrect.\nType \"help;\" to display supported commands.");
                                }
                            }
                            Constants.DataTypes type = table.getColumnType(columnName);
                            switch(type){
                                // YEAR values format : String YYYY
                                case YEAR -> {
                                    value = DataFunctions.toDbYear(value);
                                }
                                // TIME values format : hh:mm:ss
                                case TIME -> {
                                    value = DataFunctions.toDbTime(value);
                                }
                                // DATETIME values format : YYYY-MM-DD_hh:mm:ss
                                case DATETIME -> {
                                    value = DataFunctions.toDbDateTime(value);
                                }
                                //DATE values format : YYYY-MM-DD
                                case DATE-> {
                                    value = DataFunctions.toDbDateTime(value+"_00:00:00");
                                }
                                default -> {
                                }
                            }
                            if (table.update(columnName, value, operator, updateCol, updateVal))
                                System.out.print("update is successful!");
                            else
                                System.out.print("update failed!");

                        } else {
                            columnName = commandTokens.get(7);
                            operator = commandTokens.get(8);
                            value = commandTokens.get(9);
                            Constants.DataTypes type = table.getColumnType(columnName);
                            switch(type){
                                // YEAR values format : String YYYY
                                case YEAR -> {
                                    value = DataFunctions.toDbYear(value);
                                }
                                // TIME values format : hh:mm:ss
                                case TIME -> {
                                    value = DataFunctions.toDbTime(value);
                                }
                                // DATETIME values format : YYYY-MM-DD_hh:mm:ss
                                case DATETIME -> {
                                    value = DataFunctions.toDbDateTime(value);
                                }
                                //DATE values format : YYYY-MM-DD
                                case DATE-> {
                                    value = DataFunctions.toDbDateTime(value+"_00:00:00");
                                }
                                default -> {
                                }
                            }
                            try {
                                // System.out.println("test:"+ columnName+" "+value+" "+operator);
                                if (table.update(columnName, value, operator, updateCol, updateVal))
                                    System.out.print("update is successful!");
                                else
                                    System.out.print("update failed!");

                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                    else {
                        System.out.println("Query is incorrect.\nType \"help;\" to display supported commands.");
                        return;
                    }

                }
            }
            else{
                if (table.update(null, null, null, updateCol, updateVal))
                    System.out.print("update is successful!");
                else
                    System.out.print("update failed!");
            }
        }
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
               columnNum.add(table.columnNames.indexOf(column));
            }
        }
        Collections.sort(columnNum);
        //for each column
        for(Integer i : columnNum){
            int maxLength = table.columnNames.get(i).length();
            // loop through every record
            Constants.DataTypes type = table.getColumnType(table.columnNames.get(i));
            switch(type) {
                // in DB as byte , display as YYYY
                case YEAR -> {
                    maxLength = 4 > maxLength ? 4 : maxLength;
                }
                case TIME -> {
                    maxLength = 8 > maxLength ? 8 : maxLength;
                }
                case DATETIME -> {
                    maxLength = 19 > maxLength ? 19 : maxLength;
                }
                case DATE -> {
                    maxLength = 10 > maxLength ? 10 : maxLength;
                }
                default -> {
                    for (int iter = 0; iter < data.size(); i++) {
                        int len = data.get(i).getValues().get(i).toString().trim().length();
                        maxLength = len > maxLength ? len : maxLength;
                    }
                }
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
                Constants.DataTypes type = table.getColumnType(table.columnNames.get(col));
                String dataVal = new String();
                switch(type) {
                    // in DB as byte , display as YYYY
                    case YEAR -> {
                        dataVal = DataFunctions.fromDbYear((byte)data.get(rec).getValues().get(col));
                    }
                    case TIME -> {
                        dataVal = DataFunctions.fromDbTime((int)data.get(rec).getValues().get(col));
                    }
                    case DATETIME -> {
                        dataVal = DataFunctions.fromDbDateTime((long)data.get(rec).getValues().get(col));
                    }
                    case DATE -> {
                        dataVal = DataFunctions.fromDbDateTime((long)data.get(rec).getValues().get(col)).substring(0,10);
                    }
                    default -> {
                        dataVal =data.get(rec).getValues().get(col).toString().trim();
                    }
                }

                temp +=" "+ String.format("%-"+colSize.get(col)+"s",dataVal) + " |";
            }
            System.out.println(temp);
        }
        //print a line
        System.out.println(Utils.printSeparator("-",totalLength));

    }

    public static Constants.DataTypes datatypeOfStr(String s){
        return switch (s) {
            case "INT" -> Constants.DataTypes.INT;
            case "TINYINT" -> Constants.DataTypes.TINYINT;
            case "SMALLINT" -> Constants.DataTypes.SMALLINT;
            case "BIGINT" -> Constants.DataTypes.BIGINT;
            case "FLOAT" -> Constants.DataTypes.FLOAT;
            case "DOUBLE" -> Constants.DataTypes.DOUBLE;
            case "YEAR" -> Constants.DataTypes.YEAR;
            case "TIME" -> Constants.DataTypes.TIME;
            case "DATETIME" -> Constants.DataTypes.DATETIME;
            case "DATE" -> Constants.DataTypes.DATE;
            case "TEXT" -> Constants.DataTypes.TEXT;
            default -> Constants.DataTypes.NULL;
        };

    }

}
