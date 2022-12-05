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
    public Table(String tableName) throws IOException {
        this.tableName = tableName;
        this.tableFile = new TableFile(tableName ,Settings.getUserDataDirectory());
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
        File file = new File(tableName + "." + columnName );
        if (file.exists()) {
            try (IndexFile indexFile = new IndexFile(this, columnName, path)) {
                ArrayList<Integer> rowIds = indexFile.search(value, operator);
                ArrayList<Record> records = new ArrayList<>();
                for (int rowId : rowIds) {
                    records.add(tableFile.getRecord(rowId));
                }
                return records;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
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

    public Constants.DataTypes getColumnType(String columnName) {
        return columnTypes.get(columnNames.indexOf(columnName));
    }

    // Handle rowId generation in here
    public void insert(ArrayList<Object> value) {
        //

    }

    // TODO: Implement this

    public boolean delete(String columnName, String value, String operator) throws IOException{
        return true;
    }

    public boolean update(String columnName, String value, String operator, String updateColumn, String updateValue)
            throws IOException{
        return true;
    }

    public boolean dropTable(){
        return true;
    }

}
