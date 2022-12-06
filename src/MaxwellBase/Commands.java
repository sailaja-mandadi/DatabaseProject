package MaxwellBase;

import Constants.Constants;

import static java.lang.System.out;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Arrays;
import java.util.List;

public class Commands {

    /* This method determines what type of command the userCommand is and
     * calls the appropriate method to parse the userCommand String.
     */
    public static void parseUserCommand(String userCommand) throws IOException {
        out.println("Command: " + userCommand);
        /* commandTokens is an array of Strings that contains one lexical token per array
         * element. The first token can be used to determine the type of command
         * The other tokens can be used to pass relevant parameters to each command-specific
         * method inside each case statement
         */
        ArrayList<String> commandTokens = commandStringToTokenList(userCommand);

        /*
         *  This switch handles a very small list of hard-coded commands from SQL syntax.
         *  You will want to rewrite this method to interpret more complex commands.
         */
        switch (commandTokens.get(0).toLowerCase()) {
            case "show" -> show(commandTokens);
            case "select" -> parseQuery(commandTokens);
            case "create" -> {
                if (commandTokens.get(1).equalsIgnoreCase("index")) {
                    parseCreateIndex(commandTokens);
                } else {
                    parseCreateTable(commandTokens);
                }
            }
            case "insert" -> parseInsert(commandTokens);
            case "delete" -> parseDelete(commandTokens);
            case "update" -> parseUpdate(commandTokens);
            case "drop" -> dropTable(commandTokens);
            case "help" -> help();
            case "version" -> displayVersion();
            case "exit", "quit" -> Settings.setExit(true);
            default -> System.out.println("I didn't understand the command: \"" + userCommand + "\"");
        }
    }

    public static void displayVersion() {
        System.out.println("MaxwellBaseLite Version " + Settings.getVersion());
        System.out.println(Settings.getCopyright());
    }

    public static void parseCreateIndex(ArrayList<String> commandTokens) throws IOException {
        if (commandTokens.size() < 6 || !commandTokens.get(3).equals("(") || !commandTokens.get(5).equals(")")) {
            System.out.println("Command is incorrect.\nType \"help;\" to display supported commands.");
            return;
        }
        Table table = new Table(commandTokens.get(2).toLowerCase(), true);
        table.createIndex(commandTokens.get(4).toLowerCase());
    }

    public static void parseCreateTable(ArrayList<String> commandTokens) throws IOException {
        ArrayList<String> columnNames = new ArrayList<>();
        ArrayList<Constants.DataTypes> columnTypes = new ArrayList<>();
        ArrayList<Boolean> primaryKey = new ArrayList<>();
        ArrayList<Boolean> unique = new ArrayList<>();
        ArrayList<Boolean> isNull = new ArrayList<>();

        /* Extract the table name from the command string token list */
        String tableFileName = commandTokens.get(2).toLowerCase();

        Table metatable = Table.tableTable;
        Table metaColumns = Table.columnTable;
        if (Table.tableExists(tableFileName)) {
            System.out.println("Table already exists!");
            return;
        }
        //Parsing the query
        int iter = 3;
        if (!commandTokens.get(iter++).equals("(")) {
            System.out.println("Invalid Syntax: \"(\" Expected. \nType \"help;\" to display supported commands.");
            return;
        }

        while (!commandTokens.get(iter).equals(")")) {
            ArrayList<String> temp = new ArrayList<>();
            while (!commandTokens.get(iter).trim().equals(",") && !commandTokens.get(iter).trim().equals(")")) {
                temp.add(commandTokens.get(iter));
                iter++;
            }

            if (temp.size() < 2) {
                System.out.println("Invalid Syntax: Both Type and Column name required. \nType \"help;\" to display supported commands.");
                return;
            }

            boolean pri = false;
            boolean uni = false;
            boolean nullable = true;
            columnNames.add(temp.get(0).toLowerCase());
            columnTypes.add(datatypeOfStr(temp.get(1)));
            int it = 2;
            while (it < temp.size()) {
                String constraint = temp.get(it);
                if (constraint.equalsIgnoreCase("PRIMARY_KEY")) { // design: table constraint not null should be given as PRIMARY_KEY
                    pri = true;
                    nullable = false;
                    it++;
                } else if (constraint.equalsIgnoreCase("UNIQUE")) {
                    uni = true;
                    it++;
                } else if (constraint.equalsIgnoreCase("NOT_NULL")) {  // design: table constraint not null should be given as NOT_NULL
                    nullable = false;
                    it++;
                } else {
                    System.out.println("Invalid Syntax: Unknown table constraint " + constraint + ".\nType \"help;\" to display supported commands.");
                    return;
                }
            }
            primaryKey.add(pri);
            unique.add(uni);
            isNull.add(nullable);
            iter++;
            if (iter >= commandTokens.size()) break;
        }

        /*  Code to create a .tbl file to contain table data */
        Table table = new Table(tableFileName, columnNames, columnTypes, isNull, true);

        /*  Code to insert an entry in the TABLES meta-data for this new table.
         *  i.e. New row in davisbase_tables if you're using that mechanism for meta-data.
         */
        metatable.insert(new ArrayList<>(List.of(tableFileName)));

        /*  Code to insert entries in the COLUMNS meta data for each column in the new table.
         *  i.e. New rows in davisbase_columns if you're using that mechanism for meta-data.
         */
        for (int i = 0; i < columnTypes.size(); i++) {
            String isNullable;
            String columnKey;
            if (primaryKey.get(i)){
                columnKey = "PRI";
            }
            else if (unique.get(i)) columnKey = "UNI";
            else columnKey = "NULL";

            if (isNull.get(i))
                isNullable = "YES";
            else
                isNullable = "NO";

            metaColumns.insert(
                new ArrayList<>(
                    Arrays.asList(
                        tableFileName.toLowerCase(),
                        columnNames.get(i).toLowerCase(),
                        columnTypes.get(i).toString(),
                        (byte) (i + 1),
                        isNullable,
                        columnKey
                    )
                )
            );
        }
        if (primaryKey.contains(true)) {
            table.createIndex(columnNames.get(primaryKey.indexOf(true)));
        }
    }

    public static void show(ArrayList<String> commandTokens) throws IOException {
        ArrayList<Record> result;
        if (commandTokens.get(1).equalsIgnoreCase("tables")) {
            Table table = Table.tableTable;
            result = table.search(null, null, null);
            //System.out.println("records returned:"+result.size());
            Commands.display(table, result, new ArrayList<>(), true);
        } else {
            System.out.println("Command is incorrect.\nType \"help;\" to display supported commands.");
        }
    }

    public static void parseQuery(ArrayList<String> commandTokens) throws IOException {
        // Where to be handled later
        boolean allColumns = false;
        String columnName;
        String value;
        String operator;
        ArrayList<String> columns = new ArrayList<>();
        String tableName;
        ArrayList<Record> result = new ArrayList<>();
        int queryLength = commandTokens.size();
        if (queryLength == 1) {
            System.out.println("Query is incorrect.\nType \"help;\" to display supported commands.");
            return;
        }
        int i = 1;
        if (commandTokens.get(i).equals("*")) {
            allColumns = true;
            i++;
        } else {
            while (i < queryLength && !(commandTokens.get(i).equalsIgnoreCase("from"))) {
                if (!(commandTokens.get(i).equalsIgnoreCase(","))) columns.add(commandTokens.get(i));
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

        tableName = commandTokens.get(i).toLowerCase();
        if (!Table.tableExists(tableName)) {
            System.out.println("Table does not exist!");
            return;
        }
        Table table;
        try {
            if (tableName.equals(Settings.maxwellBaseTables) || tableName.equals(Settings.maxwellBaseColumns))
                table = new Table(tableName, false);
            else
                table = new Table(tableName, true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // System.out.println("test1:"+ tableName+" "+ allColumns+" "+columns);
        i++;
        if (queryLength == i) {
            try {
                //System.out.println("")
                result = table.search(null, null, null);
                //System.out.println("test2:"+ tableName+" "+ allColumns+" "+columns);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else if (commandTokens.get(i).equalsIgnoreCase("where")) {
            // System.out.println("i,test:"+ i );
            i++;
            if (i + 3 == queryLength || i + 4 == queryLength) {
                if (commandTokens.get(i).equalsIgnoreCase("not")) {
                    columnName = commandTokens.get(i + 1);
                    value = commandTokens.get(i + 3);
                    operator = negateOperator(commandTokens.get(i + 2));
                    if (operator == null) {
                        System.out.println("Query is incorrect.\nType \"help;\" to display supported commands.");
                        return;
                    }
                } else {
                    columnName = commandTokens.get(i);
                    operator = commandTokens.get(i + 1);
                    value = commandTokens.get(i + 2);
                }
                Constants.DataTypes type = table.getColumnType(columnName);
                Object valueObject = DataFunctions.parseString(type, value);
                result = table.search(columnName.toLowerCase(), valueObject, operator);
            } else {
                System.out.println("Query is incorrect.\nType \"help;\" to display supported commands.");
                return;
            }
        }
        Commands.display(table, result, columns, allColumns);

    }

    //public static ArrayList<String> condition(String a,Str)
    public static void parseInsert(ArrayList<String> commandTokens) throws IOException {
        if (commandTokens.size() < 5) {
            out.println("1Command is incorrect.\nType \"help;\" to display supported commands.");
            return;
        }

        if (!commandTokens.get(1).equalsIgnoreCase("into")) {
            out.println("2Command is incorrect.\nType \"help;\" to display supported commands.");
            return;
        }

        String tableFileName = commandTokens.get(2).toLowerCase();
        if (!Table.tableExists(tableFileName)) {
            out.println("Table " + tableFileName + " does not exist.");
            return;
        }

        Table table = new Table(tableFileName, true);
        String[] values = new String[table.columnNames.size()];
        String[][] temp = new String[table.columnNames.size()][2];
        if (!commandTokens.get(3).equals("(") && !commandTokens.get(3).equalsIgnoreCase("values")) {
            out.println("Command is incorrect.\nType \"help;\" to display supported commands.");
            return;
        }

        if (commandTokens.get(3).equals("(")) {
            int iter = 4;
            int cptr = 0;
            while (!commandTokens.get(iter).equals(")")) {
                if (!commandTokens.get(iter).equals(",")) {
                    temp[cptr++][0] = commandTokens.get(iter);
                }
                iter++;
            }
            iter++;
            if (!commandTokens.get(iter).equalsIgnoreCase("values") ||
                    !commandTokens.get(iter + 1).equals("(")) {
                out.println("Command is incorrect.\nType \"help;\" to display supported commands.");
                return;
            } else {
                iter += 2;
                cptr = 0;
                while (!commandTokens.get(iter).equals(")")) {
                    if (!commandTokens.get(iter).equals(",")) {
                        temp[cptr++][1] = commandTokens.get(iter);
                    }
                    iter++;
                }
            }

        } else if (commandTokens.get(3).equalsIgnoreCase("values")) {
            int iter = 4, vptr = 0;
            if (!commandTokens.get(iter).equals("(")) {
                out.println("Command is incorrect.\nType \"help;\" to display supported commands.");
                return;
            } else {
                iter++;
                while (!commandTokens.get(iter).equals(")")) {
                    if (!commandTokens.get(iter).equals(",")) {
                        temp[vptr][0] = table.columnNames.get(vptr);
                        temp[vptr++][1] = commandTokens.get(iter);
                    }
                    iter++;
                }
            }
        }
        // create an array of values at appropriate positions
        for (String[] strings : temp) {
            int j = table.columnNames.indexOf(strings[0]);
            values[j] = strings[1];
        }
        // for each null value , check if it can be nullable
        for (int flag = 0; flag < values.length; flag++) {
            if (values[flag] == null && !table.colIsNullable.get(flag)) {
                out.println(table.columnNames.get(flag) + "can not be NULL!");
                return;
            }
        }
        ArrayList<Object> insertValues = new ArrayList<>();
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                Constants.DataTypes type = table.columnTypes.get(i);
                Object value = DataFunctions.parseString(type, values[i]);
                insertValues.add(value);
            } else
                insertValues.add(null);
        }
        if (table.insert(insertValues)) {
            out.println("1 row inserted successfully.");
        } else {
            out.println("Insertion failed.");
        }
    }

    public static void parseDelete(ArrayList<String> commandTokens) throws IOException {
        String columnName;
        Object value;
        String operator;

        if (!commandTokens.get(0).equalsIgnoreCase("delete") || !commandTokens.get(1).equalsIgnoreCase("from")) {
            out.println("Command is Invalid");
            return;
        }

        String tableName = commandTokens.get(2).toLowerCase();
        if (!Table.tableExists(tableName)) {
            out.println("Table " + tableName + " does not exist.");
            return;
        }

        Table table = new Table(tableName, true);
        int queryLength = commandTokens.size();
        if (queryLength > 3) {
            if (!commandTokens.get(3).equalsIgnoreCase("where")) {
                out.println("Command is InValid");
                return;
            }

            if (queryLength != 7 && queryLength != 8) {
                System.out.println("Query is incorrect.\nType \"help;\" to display supported commands.");
                return;
            }

            if (commandTokens.get(4).equalsIgnoreCase("not")) {
                columnName = commandTokens.get(5).toLowerCase();
                operator = negateOperator(commandTokens.get(6));
                Constants.DataTypes type = table.getColumnType(columnName);
                value = DataFunctions.parseString(type, commandTokens.get(7));
            } else {
                columnName = commandTokens.get(4).toLowerCase();
                operator = commandTokens.get(5);
                Constants.DataTypes type = table.getColumnType(columnName);
                value = DataFunctions.parseString(type, commandTokens.get(6));
            }
        } else {
            columnName = null;
            value = null;
            operator = null;
        }

        int deletedRows = table.delete(columnName, value, operator);
        if (deletedRows > 0)
            System.out.println(deletedRows + " rows are deleted!");
        else
            System.out.println("delete failed!");
    }


    public static void dropTable(ArrayList<String> commandTokens) throws IOException {
        if (!commandTokens.get(1).equalsIgnoreCase("table")) {
            out.println("Command is Invalid");
            return;
        }

        Table table = new Table(commandTokens.get(2).toLowerCase(), true);
        if (table.dropTable()) {
            out.println("Table " + table.tableName + " is dropped.");
        } else {
            out.println("Table " + table.tableName + " was not able to be dropped.");
        }
    }


    public static void parseUpdate(ArrayList<String> commandTokens) throws IOException {
        String columnName;
        Object value;
        String operator;
        String updateCol;
        Object updateVal;
        if (!commandTokens.get(0).equalsIgnoreCase("update") || !commandTokens.get(2).equalsIgnoreCase("set")) {
            out.println("Invalid Command Syntax");
            return;
        }

        String tableName = commandTokens.get(1).toLowerCase();
        if (!Table.tableExists(tableName)) {
            out.println("Table " + tableName + " does not exist.");
            return;
        }

        Table table = new Table(commandTokens.get(1).toLowerCase(), true);
        updateCol = commandTokens.get(3);
        Constants.DataTypes updateColType = table.getColumnType(updateCol);
        updateVal = DataFunctions.parseString(updateColType, commandTokens.get(5));

        int queryLength = commandTokens.size();
        if (queryLength > 6) {
            if (!commandTokens.get(6).equalsIgnoreCase("where") && queryLength != 11 && queryLength != 10) {
                out.println("Invalid Command Syntax");
                return;
            }
            if (commandTokens.get(7).equalsIgnoreCase("not")) {
                columnName = commandTokens.get(8).toLowerCase();
                Constants.DataTypes type = table.getColumnType(columnName);
                value = DataFunctions.parseString(type, commandTokens.get(10));
                operator = negateOperator(commandTokens.get(9));
                if (operator == null) {
                    out.println("Invalid operator");
                    return;
                }
            } else {
                columnName = commandTokens.get(7).toLowerCase();
                Constants.DataTypes type = table.getColumnType(columnName);
                operator = commandTokens.get(8);
                value = DataFunctions.parseString(type, commandTokens.get(9));
            }
        } else {
            columnName = null;
            operator = null;
            value = null;
        }
        int updated = table.update(columnName, value, operator, updateCol, updateVal);
        if (updated > 0)
            System.out.println(updated + " rows updated!");
        else
            System.out.println("update failed!");
    }

    public static ArrayList<String> commandStringToTokenList(String command) {
        command = command.replaceAll("\n", " ");    // Remove newlines
        command = command.replaceAll("\r", " ");    // Remove carriage returns
        command = command.replaceAll(",", " , ");   // Tokenize commas
        command = command.replaceAll("\\(", " ( "); // Tokenize left parentheses
        command = command.replaceAll("\\)", " ) "); // Tokenize right parentheses
        command = command.replaceAll("( )+", " ");  // Reduce multiple spaces to a single space
        return new ArrayList<>(Arrays.asList(command.split(" ")));
    }

    /**
     * Help: Display supported commands
     */
    public static void help() {
        out.println(Utils.printSeparator("*", 80));
        out.println("SUPPORTED COMMANDS\n");
        out.println("All commands below are case insensitive\n");
        out.println("SHOW TABLES;");
        out.println("\tDisplay the names of all tables.\n");
        out.println("SELECT column_list FROM table_name [WHERE condition];\n");
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
        out.println(Utils.printSeparator("*", 80));
    }


    /**
     * function to display records returned in a search query as a Table in command line
     *
     * @param table      - name of table
     * @param data       - list of records that are result of a search query
     * @param selColumns - columns to be displayed as per select query
     * @param allColumns - boolean to know if there is a "*" wildcard in select query
     */
    public static void display(Table table, ArrayList<Record> data, ArrayList<String> selColumns, boolean allColumns) {
        ArrayList<Integer> columnNum = new ArrayList<>();
        ArrayList<Integer> colSize = new ArrayList<>();
        if (allColumns) {
            for (int i = 0; i < table.columnNames.size(); i++)
                columnNum.add(i);
        } else {
            for (String column : selColumns) {
                columnNum.add(table.columnNames.indexOf(column.toLowerCase()));
            }
        }
        Collections.sort(columnNum);
        //for each column
        for (Integer i : columnNum) {
            int maxLength = table.columnNames.get(i).length();
            // loop through every record
            Constants.DataTypes type = table.getColumnType(table.columnNames.get(i));
            maxLength = switch (type) {
                // in DB as byte , display as YYYY
                case YEAR -> Math.max(4, maxLength);
                case TIME -> Math.max(8, maxLength);
                case DATETIME -> Math.max(19, maxLength);
                case DATE -> Math.max(10, maxLength);
                default -> {
                    int temp = maxLength;
                    for (Record datum : data) {
                        int len;
                        Object val = datum.getValues().get(i);
                        if (val != null)
                            len = val.toString().trim().length();
                        else
                            len = 4;
                        temp = Math.max(len, temp);
                    }
                    yield temp;
                }
            };
            colSize.add(maxLength);
        }

        //length of column separators
        int totalLength = (columnNum.size() - 1) * 3 + 4;
        //for (int i: colNum) - changed
        for (Integer integer : colSize) {
            totalLength += integer;
        }

        //print a line
        System.out.println(Utils.printSeparator("-", totalLength));
        //print column names
        StringBuilder temp = new StringBuilder("|");
        for (Integer i : columnNum) {
            temp.append(" ").append(String.format("%-" + colSize.get(i) + "s", table.columnNames.get(i))).append(" |");
        }
        System.out.println(temp);
        //print a line
        System.out.println(Utils.printSeparator("-", totalLength));
        // print data
        for (Record datum : data) {
            temp = new StringBuilder("|");
            for (Integer col : columnNum) {
                Constants.DataTypes type = table.getColumnType(table.columnNames.get(col));
                Object val = datum.getValues().get(col);
                String dataVal = DataFunctions.valueToString(type, val);
                temp.append(" ").append(String.format("%-" + colSize.get(col) + "s", dataVal)).append(" |");
            }
            System.out.println(temp);
        }
        //print a line
        System.out.println(Utils.printSeparator("-", totalLength));

    }

    public static Constants.DataTypes datatypeOfStr(String s) {
        return switch (s.toUpperCase()) {
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

    public static String negateOperator(String operator) {
        return switch (operator) {
            case "=" -> "!=";
            case "!=" -> "=";
            case ">" -> "<=";
            case "<" -> ">=";
            case ">=" -> "<";
            case "<=" -> ">";
            default -> null;
        };
    }
}
