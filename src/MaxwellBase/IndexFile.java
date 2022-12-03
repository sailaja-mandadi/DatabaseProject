package MaxwellBase;

import Constants.Constants;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

public class IndexFile extends DatabaseFile{
    Constants.DataTypes dataType;
    short valueSize;

    /**
     * Creates a new IndexFile object
     * @param table The table that the index file is for
     * @param columnName The column that the index file indexes
     * @throws IOException
     */
    public IndexFile(Table table, String columnName) throws IOException {
        super(table.tableName + "." + columnName + ".ndx", Constants.PageType.INDEX_LEAF);
        this.dataType = table.getColumnType(columnName);
        switch (dataType) {
            case TINYINT, YEAR -> this.valueSize = 1;
            case SMALLINT -> this.valueSize = 2;
            case INT, TIME, FLOAT -> this.valueSize = 4;
            case BIGINT, DATE, DATETIME, DOUBLE -> this.valueSize = 8;
            case TEXT -> this.valueSize = -1;
            case NULL -> this.valueSize = 0;
        }
    }

    /**
     * Split the page into two pages
     * Move half of the records to the new page and middle record to the parent page
     * @param pageNumber The page to split
     * @return The new page that was created
     */
    public int splitPage(int pageNumber, Object splittingValue) throws IOException {
        this.seek((long) pageNumber * pageSize);
        byte pageTypeByte = this.readByte();
        Constants.PageType pageType = Constants.PageType.fromValue(pageTypeByte);

        int parentPage = getParentPage(pageNumber);
        if (parentPage == 0xFFFFFFFF) {
            parentPage = createPage(0xFFFFFFFF, Constants.PageType.INDEX_INTERIOR);
            this.seek((long) pageNumber * pageSize + 0x0A);
            this.writeInt(parentPage);
        }

        int newPage = createPage(parentPage, pageType);
        int middleRecord = getNumberOfCells(pageNumber) / 2;
        this.seek((long) pageNumber * pageSize + 0x10 + middleRecord * 2);
        int middleRecordOffset = this.readShort();

    }

    /**
     * Initializes the index file with all the records in the table
     * @param table the table that the index is being created for
     * @param columnName the column that the index indexes
     */
    public void InitializeIndex(Table table, String columnName) throws IOException {

    }

    /**
     * Writes a record to the index file at a specific location
     * Format: [number of records: 1 byte][data type: 1 byte][value: N bytes][array of rowids: 4*len(rowids) bytes]
     * @param value The value of the column this cell is for
     * @param rowIds The row ids that have this value
     * @param page The page to write to
     */
    public void writeCell(Object value, ArrayList<Integer> rowIds, int page) throws IOException {
        // get cell size
        short cellSize = (short) (2 + valueSize + 4 * rowIds.size());
        if (valueSize == -1) {
            cellSize += ((String) value).length() + 1;
        }
        if (shouldSplit(page, cellSize)) {
            page = splitPage(page);
        }
        int contentStart = setContentStart(page, cellSize);
        int numberOfCells = incrementNumberOfCells(page);
        // write cell start to cell pointer array
        this.seek((long) page * pageSize + 0x03 + 2 * numberOfCells);
        this.writeShort(contentStart);
        this.seek((long) page * pageSize + contentStart);
        // write number of records
        this.writeByte(rowIds.size());
        // write data type
        this.writeByte(dataType.ordinal());
        // write value
        switch (dataType) {
            case TINYINT, YEAR -> this.writeByte((Byte) value);
            case SMALLINT -> this.writeShort((Short) value);
            case INT, TIME -> this.writeInt((Integer) value);
            case BIGINT, DATE, DATETIME-> this.writeLong((Long) value);
            case FLOAT -> this.writeFloat((Float) value);
            case DOUBLE -> this.writeDouble((Double) value);
            case TEXT -> this.writeBytes((String) value);
        }
        // write row ids
        for (int rowId : rowIds) {
            this.writeInt(rowId);
        }
    }


    /**
     * Update the index file after a record is modified
     * @param rowId The row id of the record that was modified
     * @param oldValue The old value of the record, null if the record was inserted
     * @param newValue The new value of the record, null if the record was deleted
     */
    public void update(int rowId, Object oldValue, Object newValue) throws IOException {

    }

    /**
     * Remove a record from a cell
     * @param value The value of the cell to remove the record from
     * @param rowId The row id of the record to remove
     */
    public void removeItemFromCell(Object value, int rowId) {

    }

    /**
     * Add a record to a cell
     * @param value The value of the cell to add the record to
     * @param rowId The row id of the record to add
     */
    public void addItemToCell(Object value, int rowId) {

    }

    public int[] findValue(Object value) throws IOException {

        return new int[] {-1, -1};
    }

    public ArrayList<Integer> search(String value, String operator) throws IOException {
        return null;
    }
}
