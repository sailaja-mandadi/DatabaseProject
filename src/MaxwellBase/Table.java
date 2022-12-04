package MaxwellBase;

import Constants.Constants;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class Table {
    ArrayList<String> columnNames;
    ArrayList<Constants.DataTypes> columnTypes;
    String tableName;
    TableFile tableFile;
    public Table(String tableName) throws IOException {
        this.tableName = tableName;
        this.tableFile = new TableFile(tableName + ".tbl",Settings.getUserDataDirectory());
    }
    public Table(String tableName, ArrayList<String> columnNames, ArrayList<Constants.DataTypes> columnTypes, boolean userDataTable) throws IOException {
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        try {
            if(userDataTable)
                this.tableFile = new TableFile(tableName + ".tbl",Settings.getUserDataDirectory());
            else
                this.tableFile = new TableFile(tableName + ".tbl",Settings.getCatalogDirectory());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public ArrayList<Record> search(String columnName, String value, String operator) throws IOException{
        // Check if index exists for columnName
        // If it does, use index to search
        // If it doesn't, use tableFile to search
        File file = new File(tableName + "." + columnName + ".ndx");
        if (file.exists()) {
            try (IndexFile indexFile = new IndexFile(this, columnName)) {
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
    public void remove() {

    }

}
