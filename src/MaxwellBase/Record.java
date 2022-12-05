package MaxwellBase;

import java.util.ArrayList;

import Constants.Constants;
public class Record {
    private short recordSize;
    private ArrayList<Constants.DataTypes> columns;
    private ArrayList<Object> values;
    private int rowId;
    private byte[] header;

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
            }
            else if (column == Constants.DataTypes.TINYINT) {
                recordSize += 1;
            } else if (column == Constants.DataTypes.SMALLINT) {
                recordSize += 2;
            } else if (column == Constants.DataTypes.INT || column == Constants.DataTypes.FLOAT) {
                recordSize += 4;
            } else if (column == Constants.DataTypes.BIGINT
                    || column == Constants.DataTypes.DOUBLE
                    || column == Constants.DataTypes.DATETIME
                    || column == Constants.DataTypes.DATE) {
                recordSize += 8;
            } else if (column == Constants.DataTypes.YEAR) {
                recordSize += 1;
            } else if (column == Constants.DataTypes.TIME) {
                recordSize += 4;
            } else if (column == Constants.DataTypes.TEXT) {
                recordSize += ((String) value).length();
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

    public boolean compare(int columnIndex, String value, String operator) {
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
        if (!(other instanceof Record)) {
            return false;
        }
        Record otherRecord = (Record) other;
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
