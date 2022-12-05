package MaxwellBase;

import Constants.Constants;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class Table {
    ArrayList<String> columnNames;
    ArrayList<Constants.DataTypes> columnTypes;
    ArrayList<Boolean> colIsNullable;
    String tableName;
    TableFile tableFile;
    String path;

    /**
     * changed signature, to include if the table is a user table or not - to search in correct path
     * @param tableName
     * @param userTable
     * @throws IOException
     */
    public Table(String tableName,boolean userTable) throws IOException {
        this.tableName = tableName;
        if(userTable)
            this.tableFile = new TableFile(tableName ,Settings.getUserDataDirectory());
        else
            this.tableFile = new TableFile(tableName ,Settings.getCatalogDirectory());
    }
    public Table(String tableName, ArrayList<String> columnNames, ArrayList<Constants.DataTypes> columnTypes,
                 ArrayList<Boolean> colIsNullable, boolean userDataTable) {
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.colIsNullable = colIsNullable;
        if(userDataTable)
            path = Settings.getUserDataDirectory();
        else
            path = Settings.getCatalogDirectory();
        try {
            tableFile = new TableFile(tableName, path);
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
        //handle rowid
        Record rec = new Record(this.columnTypes, values, nextRowId);
        tableFile.appendRecord(rec);

        // Update indexes
        for (int i = 0; i < columnNames.size(); i++) {
            if (indexExists(columnNames.get(i))) {
                getIndexFile(columnNames.get(i)).addItemToCell(values.get(i), nextRowId);
            }
        }
    }

    // TODO: Implement this

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

    public int update(String searchColumn, String searchValue, String operator, String updateColumn, String updateValue)
            throws IOException{

        int columnIndex;
        if (updateColumn != null) {
            columnIndex = columnNames.indexOf(updateColumn);
        } else {
            throw new RuntimeException("Column name cannot be null");
        }
        // Check if index exists for columnName
        if (indexExists(searchColumn)) {
            ArrayList<Integer> recordIds = getIndexFile(searchColumn).search(updateValue, operator);
            for (int recordId : recordIds) {
                Record record = tableFile.getRecord(recordId);
                tableFile.updateRecord(recordId, columnIndex, updateValue);
                if (indexExists(updateColumn)) {
                    IndexFile indexFile = getIndexFile(updateColumn);
                    Object oldValue = record.getValues().get(columnIndex);
                    indexFile.removeItemFromCell(oldValue, recordId);
                    indexFile.addItemToCell(updateValue, recordId);
                }
            }
            return recordIds.size();
        }
        else {
            return tableFile.updateRecords(columnIndex, updateValue, operator, searchColumn, searchValue);
        }


        return 0;
    }

    public boolean dropTable(){
        return true;
    }

    public boolean indexExists(String columnName) {
        File file = new File(path + "/" + tableName + "." + columnName + ".ndx");
        return file.exists();
    }



}
