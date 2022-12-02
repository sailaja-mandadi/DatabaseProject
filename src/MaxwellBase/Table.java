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

    public Table(String tableName) {
        this.tableName = tableName;
        try {
            this.tableFile = new TableFile(tableName + ".tbl");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public ArrayList<Record> search(String columnName, String value, String operator) {
        // Check if index exists for columnName
        // If it does, use index to search
        // If it doesn't, use tableFile to search
        File file = new File(tableName + "." + columnName + ".ndx");
        if (file.exists()) {
            try {
                IndexFile indexFile = new IndexFile(tableName + "." + columnName + ".ndx");
                ArrayList<Integer> rowIds = indexFile.search(value, operator);
                ArrayList<Record> records = new ArrayList<>();
                for (int rowId : rowIds) {
                    records.add(tableFile.readRecord(rowId));
                }
                return records;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        else {
            int columnIndex = columnNames.indexOf(columnName);
            return tableFile.search(columnIndex, value, operator);
        }



        return null;
    }


}
