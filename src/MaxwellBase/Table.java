package MaxwellBase;

import Constants.Constants;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

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
     * @param tableName
     * @param userTable
     * @throws IOException
     */
    public Table(String tableName,boolean userTable) throws IOException {
        this.tableName = tableName;
        if(userTable) {
            this.tableFile = new TableFile(tableName ,Settings.getUserDataDirectory());
            this.path = Settings.getUserDataDirectory();
        }
        else {
            this.tableFile = new TableFile(tableName ,Settings.getCatalogDirectory());
            this.path = Settings.getCatalogDirectory();
        }
        loadTable(tableName);

    }

    public void loadTable(String tableName) throws IOException {
        ArrayList<Record> tables = tableTable.search("table_name", tableName, "=");
        if (tables.size() == 0) {
            return;
        } if (tables.size() > 1) {
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

    public Table(String tableName, ArrayList<String> columnNames, ArrayList<Constants.DataTypes> columnTypes,
                 ArrayList<Boolean> colIsNullable, boolean userDataTable) {
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.colIsNullable = colIsNullable;
        if(userDataTable)
            this.path = Settings.getUserDataDirectory();
        else
           this.path = Settings.getCatalogDirectory();
        try {
            tableFile = new TableFile(tableName, this.path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public ArrayList<Record> search(String columnName, String value, String operator) throws IOException{
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
        }
        else {
            int columnIndex;
            if (columnName != null) {
                columnIndex = columnNames.indexOf(columnName);
            } else {
                columnIndex = -1;
            }
            return tableFile.search(columnIndex, value, operator);
        }
    }
    public IndexFile getIndexFile(String columnName) throws IOException {
        File indexFile = new File(path + "/" + tableName + "." + columnName + ".ndx");
        if (indexFile.exists()) {
            return new IndexFile(this, columnName, path);
        }
        return null;
    }

    public Constants.DataTypes getColumnType(String columnName) {
        return columnTypes.get(columnNames.indexOf(columnName));
    }

    // Handle rowId generation in here
    public void insert(ArrayList<Object> values) throws IOException {
        int nextRowId = tableFile.getLastRowId() + 1;
        ArrayList<Constants.DataTypes> types = new ArrayList<>(columnTypes);

        //handle rowid
        for (int i = 0; i < columnNames.size(); i++) {
            Object value = values.get(i);
            if (value == null) {
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

    // TODO: Implement this

    /**
     *
     * @param columnName
     * @param value
     * @param operator
     * @return No of rows deleted
     * @throws IOException
     */

    public int delete(String columnName, String value, String operator) throws IOException{
        // Check if index exists for columnName
        if (indexExists(columnName)) {
            IndexFile indexFile = getIndexFile(columnName);
            ArrayList<Integer> recordIds = indexFile.search(value, operator);
            for (int recordId : recordIds) {
                tableFile.deleteRecord(recordId);
                indexFile.removeItemFromCell(value, recordId);
            }
            return recordIds.size();
        }
        else {
            int columnIndex;
            if (columnName != null) {
                columnIndex = columnNames.indexOf(columnName);
            } else {
                columnIndex = -1;
            }
            return tableFile.deleteRecords(columnIndex, value, operator);
        }
    }

    /**
     *
     * @param searchColumn
     * @param searchValue
     * @param operator
     * @param updateColumn
     * @param updateValue
     * @return no of rows updated
     * @throws IOException
     */
    public int update(String searchColumn, String searchValue, String operator, String updateColumn, String updateValue)
            throws IOException{

        int columnIndex;
        if (updateColumn != null) {
            columnIndex = columnNames.indexOf(updateColumn);
        } else {
            throw new RuntimeException("Column name cannot be null");
        }
        int searchColumnIndex;
        if (searchColumn != null) {
            searchColumnIndex = columnNames.indexOf(searchColumn);
        } else {
            searchColumnIndex = -1;
        }
        ArrayList<Record> records;
        // Check if index exists for columnName
        if (indexExists(searchColumn)) {
            ArrayList<Integer> recordIds = getIndexFile(searchColumn).search(updateValue, operator);
            records = new ArrayList<>();
            for (int recordId : recordIds) {
                Record record = tableFile.getRecord(recordId);
                records.add(record);
            }
            return recordIds.size();
        }
        else {
             records = tableFile.search(searchColumnIndex, searchValue, operator);
        }
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
     *
     * @return
     */
    public boolean dropTable() {
       // java file.delete
        try {
            tableTable.delete("table_name",this.tableName,"=");
            columnTable.delete("table_name",this.tableName,"=");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        File tableFile = new File(path + "/" + tableName + ".tbl");
        return tableFile.delete();
    }

    public boolean indexExists(String columnName) {
        File file = new File(path + "/" + tableName + "." + columnName + ".ndx");
        return file.exists();
    }

    public void createIndex(String columnName) {
        try {
            IndexFile indexFile = getIndexFile(columnName);

            if (indexFile == null) {
                indexFile = new IndexFile(this, columnName, path);
                indexFile.initializeIndex();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }



}
