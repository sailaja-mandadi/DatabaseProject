package MaxwellBase;

import Constants.Constants;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class Table {
    ArrayList<String> columnNames;
    ArrayList<Constants.DataTypes> columnTypes;
    ArrayList<Boolean> colIsNullable;
    String tableName;
    TableFile tableFile;
    String path;

    public static Table tableTable;
    public static Table columnTable;

    /**
     * changed signature, to include if the table is a user table or not - to search in correct path
     *
     * @param tableName name of the table
     * @param userTable whether the table is a user table or meta table
     */
    public Table(String tableName, boolean userTable) throws IOException {
        this.tableName = tableName;
        if (userTable) {
            this.tableFile = new TableFile(tableName, Settings.getUserDataDirectory());
            this.path = Settings.getUserDataDirectory();
        } else {
            this.tableFile = new TableFile(tableName, Settings.getCatalogDirectory());
            this.path = Settings.getCatalogDirectory();
        }
        loadTable(tableName);
    }

    /**
     * Constructor for Table
     * @param tableName name of the table to be created
     * @param columnNames names of the columns
     * @param columnTypes types of the columns
     * @param colIsNullable which columns are nullable
     * @param userDataTable whether the table is a user table or meta table
     */
    public Table(String tableName, ArrayList<String> columnNames, ArrayList<Constants.DataTypes> columnTypes,
                 ArrayList<Boolean> colIsNullable, boolean userDataTable) {
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.colIsNullable = colIsNullable;
        if (userDataTable)
            this.path = Settings.getUserDataDirectory();
        else
            this.path = Settings.getCatalogDirectory();
        try {
            tableFile = new TableFile(tableName, this.path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean tableExists(String tableName) {
        ArrayList<Record> tables = null;
        try {
            tables = tableTable.search("table_name", tableName, "=");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return tables.size() != 0;
    }

    /**
     * Loads the table settings from metadata tables
     * @param tableName name of the table to be loaded
     */
    public void loadTable(String tableName) throws IOException {
        ArrayList<Record> tables = tableTable.search("table_name", tableName, "=");
        if (tables.size() == 0) {
            return;
        }
        if (tables.size() > 1) {
            throw new RuntimeException("More than one table with the same name");
        }
        ArrayList<Record> columns = columnTable.search("table_name", tableName, "=");
        columnNames = new ArrayList<>();
        columnTypes = new ArrayList<>();
        colIsNullable = new ArrayList<>();
        for (Record column : columns) {
            columnNames.add((String) column.getValues().get(1));
            columnTypes.add(Constants.DataTypes.valueOf((String) column.getValues().get(2)));
            colIsNullable.add(column.getValues().get(4) == "YES");
        }
    }

    /**
     * Search the table based on value and operator; use index for column if available
     * @param columnName name of the column to be searched
     * @param value value to search for
     * @param operator operator to be used for comparison
     * @return a list of records, an empty list
     */
    public ArrayList<Record> search(String columnName, Object value, String operator) throws IOException {
        // Check if index exists for columnName
        // If it does, use index to search
        // If it doesn't, use tableFile to search
        if (indexExists(columnName)) {
            IndexFile indexFile = getIndexFile(columnName);
            ArrayList<Integer> recordIds = indexFile.search(value, operator);
            ArrayList<Record> records = new ArrayList<>();
            for (int recordId : recordIds) {
                records.add(tableFile.getRecord(recordId));
            }
            return records;
        } else {
            int columnIndex;
            if (columnName != null && columnNames.contains(columnName)) {
                columnIndex = columnNames.indexOf(columnName);
            } else if (columnName == null) {
                columnIndex = -1;
            } else {
                return new ArrayList<>();
            }
            return tableFile.search(columnIndex, value, operator);
        }
    }

    /**
     * Gets the Index file if it exists
     * @param columnName column name to get the index file for
     * @return an index file if it exists, null otherwise
     */
    public IndexFile getIndexFile(String columnName){
        File indexFile = new File(path + "/" + tableName + "." + columnName + ".ndx");
        if (indexFile.exists()) {
            try {
                return new IndexFile(this, columnName, path);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return null;
    }

    /**
     * Get ths type of column
     * @param columnName name of the column
     * @return "PRI", "UNI", or NULL
     */
    public Constants.DataTypes getColumnType(String columnName) {
        return columnTypes.get(columnNames.indexOf(columnName));
    }


    /**
     * Inserts the values into the table; This is where rowid generation is handled
     * @param values values to be inserted
     */
    public void insert(ArrayList<Object> values) throws IOException {
        int nextRowId = tableFile.getLastRowId() + 1;
        ArrayList<Constants.DataTypes> types = new ArrayList<>(columnTypes);

        for (int i = 0; i < columnNames.size(); i++) {
            if (values.get(i) == null) {
                types.set(i, Constants.DataTypes.NULL);
            }
        }
        Record rec = new Record(types, values, nextRowId);
        tableFile.appendRecord(rec);

        // Update indexes
        for (int i = 0; i < columnNames.size(); i++) {
            if (indexExists(columnNames.get(i))) {
                getIndexFile(columnNames.get(i)).addItemToCell(values.get(i), nextRowId);
            }
        }
    }

    /**
     * @param columnName column used to search for records to delete
     * @param value     value to search for
     * @param operator operator to use in search
     * @return No of rows deleted
     */
    public int delete(String columnName, Object value, String operator) throws IOException {
        ArrayList<Record> records = search(columnName, value, operator);
        for (Record record : records) {
            tableFile.deleteRecord(record.getRowId());
            for (int i = 0; i < columnNames.size(); i++) {
                if (indexExists(columnNames.get(i))) {
                    getIndexFile(columnNames.get(i)).removeItemFromCell(record.getValues().get(i), record.getRowId());
                }
            }
        }
        return records.size();
    }

    /**
     * @param searchColumn  Column to condition update on
     * @param searchValue  Value to condition update on
     * @param operator    Operator to condition update on
     * @param updateColumn Column to update
     * @param updateValue New value to write
     * @return no of rows updated
     */
    public int update(String searchColumn, Object searchValue, String operator,
                      String updateColumn, Object updateValue) throws IOException {

        int columnIndex;
        if (columnNames.contains(updateColumn)) {
            columnIndex = columnNames.indexOf(updateColumn);
        } else {
            return 0;
        }
        ArrayList<Record> records = search(searchColumn, searchValue, operator);
        for (Record record : records) {
            tableFile.updateRecord(record.getRowId(), columnIndex, updateValue);
            if (indexExists(updateColumn)) {
                IndexFile indexFile = getIndexFile(updateColumn);
                indexFile.addItemToCell(updateValue, record.getRowId());
                indexFile.removeItemFromCell(record.getValues().get(columnIndex), record.getRowId());
            }
        }
        return records.size();
    }

    /**
     * Drops the table, and delete corresponding meta data and indexes.
     * @return true if table is dropped, false otherwise
     */
    public boolean dropTable() {
        // java file.delete
        try {
            tableTable.delete("table_name", this.tableName, "=");
            columnTable.delete("table_name", this.tableName, "=");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        File tableFile = new File(path + "/" + tableName + ".tbl");
        return tableFile.delete();
    }

    /**
     * Checks if the index file exists
     * @param columnName column name to check for index
     * @return True if exists, False is not exists.
     */
    public boolean indexExists(String columnName) {
        File file = new File(path + "/" + tableName + "." + columnName + ".ndx");
        return file.exists();
    }


    /**
     * Creates an index file for the column
     * @param columnName column name to create index for
     */
    public void createIndex(String columnName) {
        if (indexExists(columnName)) {
            return;
        }
        try (IndexFile indexFile = new IndexFile(this, columnName, path)) {
            indexFile.initializeIndex();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}
