package MaxwellBase;

import java.util.ArrayList;

import Constants.Constants;
public class Record {
    private short recordSize;
    private final ArrayList<Constants.DataTypes> columns;
    private final ArrayList<Object> values;
    private final int rowId;
    private final byte[] header;

    public Record(ArrayList<Constants.DataTypes> columns, ArrayList<Object> values, int rowId) {
        this.columns = columns;
        this.values = values;
        this.rowId = rowId;
        this.header = new byte[1 + (columns.size())];
        this.header[0] = (byte) columns.size();
        this.recordSize = (short) (1 + (columns.size()));
        for (int i = 0; i < columns.size(); i++) {
            var column = columns.get(i);
            var value = values.get(i);
            if (value == null) {
                column = Constants.DataTypes.NULL;
            } else {
                int size = DataFunctions.typeSize(column);
                recordSize += size != -1 ? size : ((String) value).length();
            }

            if (column != Constants.DataTypes.TEXT) {
                header[i + 1] = (byte) column.ordinal();
            } else {
                header[i + 1] = (byte) (column.ordinal() + ((String) value).length());
            }
        }
    }

    public short getRecordSize() {
        return recordSize;
    }

    public byte[] getHeader() {
        return header;
    }

    public ArrayList<Constants.DataTypes> getColumns() {
        return columns;
    }
    public ArrayList<Object> getValues() {
        return values;
    }

    public int getRowId() {
        return rowId;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Record size: ").append(recordSize).append("\n\t");
        sb.append("Row id: ").append(rowId).append("\n\t");
        for (int i = 0; i < columns.size(); i++) {
            sb.append(columns.get(i)).append(": ").append(values.get(i).toString()).append("\n\t");
        }
        return sb.toString();
    }

    public boolean compare(int columnIndex, Object value, String operator) {
        if (columnIndex == -1 && value == null && operator == null) {
            return true;
        }
        Constants.DataTypes columnType = columns.get(columnIndex);
        if (columnType == Constants.DataTypes.NULL || value == null) {
            return false;
        }
        Object columnValue = values.get(columnIndex);
        return DataFunctions.compare(columnType, columnValue, value, operator);
    }

    public boolean equals(Object other) {
        if (!(other instanceof Record otherRecord)) {
            return false;
        }
        if (this.columns.size() != otherRecord.columns.size()) {
            return false;
        }
        for (int i = 0; i < this.columns.size(); i++) {
            if (this.columns.get(i) != otherRecord.columns.get(i)) {
                return false;
            }
            if (!this.values.get(i).equals(otherRecord.values.get(i))) {
                return false;
            }
        }
        return true;
    }
}
