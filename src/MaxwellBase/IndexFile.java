package MaxwellBase;

import Constants.Constants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

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

    public Object readValue(int page, int offset) throws IOException{
        this.seek((long) page * Constants.PAGE_SIZE);
        Constants.PageType pageType = Constants.PageType.values()[this.readByte()];
        this.seek((long) page * pageSize + offset + 2 + 1);
        if (pageType == Constants.PageType.INDEX_INTERIOR) {
            this.skipBytes(4);
        }
        byte recordType = this.readByte();
        return switch (dataType) {
            case TINYINT, YEAR -> this.readByte();
            case SMALLINT -> this.readShort();
            case INT, TIME -> this.readInt();
            case BIGINT, DATE, DATETIME -> this.readLong();
            case FLOAT -> this.readFloat();
            case DOUBLE -> this.readDouble();
            case TEXT -> {
                int textLength = recordType - 0x0C;
                byte[] text = new byte[textLength];
                this.read(text);
                yield new String(text);
            }
            default -> null;
        };
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
        int middleRecordOffset = getCellOffset(pageNumber, middleRecord);

        // Read the middle record and write it to the parent page
        this.seek((long) pageNumber * pageSize + middleRecordOffset);
        if (pageType == Constants.PageType.INDEX_INTERIOR) {
            this.readInt();
        }
        short payloadSize = this.readShort();
        byte middleRecordSize = this.readByte();
        Object middleRecordValue = readValue(pageNumber, middleRecordOffset);
        ArrayList<Integer> middleRecordPointers = new ArrayList<>();
        for (int i = 0; i < middleRecordSize; i++) {
            middleRecordPointers.add(this.readInt());
        }
        int middleRecordIndex = this.writeCell(middleRecordValue, middleRecordPointers, parentPage, pageNumber);

        // Fill space where the middle record was with 0s
        this.seek((long) pageNumber * pageSize + middleRecordOffset);
        int cellSize = payloadSize + 2 + (pageType == Constants.PageType.INDEX_INTERIOR ? 4: 0);
        for (int i = 0; i < cellSize; i++) {
            this.writeByte(0);
        }

        // Update the pointers in the parent page
        int nextPointer = getCellOffset(parentPage, middleRecordIndex + 1);
        this.seek((long) parentPage * pageSize + nextPointer);
        this.writeInt(newPage);

        // Move the cells after the middle record to the new page
        this.moveCells(pageNumber, newPage, middleRecord + 1);

        Object middleValue = readValue(pageNumber, middleRecordOffset);

        if (DataFunctions.compareTo(dataType, middleValue, splittingValue) > 0) {
            return newPage;
        } else {
            return pageNumber;
        }
    }

    /**
     * Shifts all cells after precedingCell on page to the front of the page by shift bytes
     * @param page The page to shift cells on
     * @param precedingCell The cell to before the first cell to be shifted
     * @param shift The number of bytes to shift the cells
     * @return The page offset of the free space created
     */
    public int shiftCells(int page, int precedingCell, int shift) throws IOException {
        if (shouldSplit(page, shift)) {
            throw new IOException("Asked to shift cells more than the page can hold");
        }

        int contentOffset = getContentStart(page);
        int startOffset;
        if (precedingCell >= 0) {
            startOffset = getCellOffset(page, precedingCell);
        } else {
            startOffset = pageSize;
        }
        this.seek((long) page * pageSize + startOffset);
        byte[] shiftedBytes = new byte[startOffset - contentOffset];
        this.read(shiftedBytes);

        this.seek((long) page * pageSize + contentOffset - shift);
        this.write(shiftedBytes);

        this.seek((long) page * pageSize + 0x10 + (precedingCell + 1) * 2L);
        int numberOfCells = getNumberOfCells(page);
        byte[] oldOffsets = new byte[(numberOfCells - precedingCell - 1) * 2];
        this.read(oldOffsets);
        this.seek((long) page * pageSize + 0x10 + (precedingCell + 2) * 2L);
        for (int i = 0; i < oldOffsets.length; i += 2) {
            short oldOffset = (short) ((oldOffsets[i] << 8) | (oldOffsets[i + 1] & 0xFF));
            this.writeShort(oldOffset - shift);
        }
        return startOffset - shift;
    }

    /**
     * Finds the index of the last cell on the page that has a value less than or equal to the given value
     * Used to search for cells, find insertion points, and find parent cells
     * @param value The value to compare against
     * @param page The page to search
     * @return The index of the last cell on the page that has a value less than or equal to the given value
     */
    public int findValuePosition(Object value, int page) throws IOException {
        int numberOfCells = getNumberOfCells(page);
        int mid = numberOfCells / 2;
        int low = -1;
        int high = numberOfCells - 1;
        int currentOffset = getCellOffset(page, mid);
        while (low < high) {
            Object currentValue = readValue(page, currentOffset);
            int comparison = DataFunctions.compareTo(dataType, currentValue, value);
            if (comparison == 0) {
                return mid;
            } else if (comparison < 0) {
                low = mid;
            } else {
                high = mid - 1;
            }
            mid =  (low + high + 1) / 2;
            currentOffset = getCellOffset(page, mid);
        }
        return mid;
    }

    /**
     * Moves all cells after precedingCell on sourcePage to destinationPage
     * @param sourcePage The page to move cells from
     * @param destinationPage The page to move cells to, Assumes that destinationPage is empty
     * @param precedingCell The cell before the first cell to be moved
     */
    public void moveCells(int sourcePage, int destinationPage, int precedingCell) throws IOException {
        int cellOffset = getCellOffset(sourcePage, precedingCell);
        int contentStart = getContentStart(sourcePage);
        // Read the bytes to be moved
        byte[] cellBytes = new byte[contentStart - cellOffset];
        this.seek((long) sourcePage * pageSize + cellOffset);
        this.read(cellBytes);

        // Read the offsets of the cells to be moved
        byte[] cellOffsets = new byte[(getNumberOfCells(sourcePage) - precedingCell - 1) * 2];
        this.seek((long) destinationPage * pageSize + 0x10 + (precedingCell + 1) * 2L);
        this.read(cellOffsets);

        // Write the bytes to be moved
        int newContentStart = getContentStart(destinationPage) - (contentStart - cellOffset);
        this.seek((long) destinationPage * pageSize + newContentStart);
        this.write(cellBytes);

        // Write the offsets of the cells to be moved
        int offsetDiff = contentStart - newContentStart;
        this.seek((long) destinationPage * pageSize + 0x10);
        for (int i = 0; i < cellOffsets.length; i += 2) {
            short offset = (short) ((cellOffsets[i] << 8) | (cellOffsets[i + 1] & 0xFF));
            this.writeShort(offset - offsetDiff);
        }
    }


    /**
     * Initializes the index file with all the records in the table
     * @param table the table that the index is being created for
     * @param columnName the column that the index indexes
     */
    public void initializeIndex(Table table, String columnName) throws IOException {

    }

    /**
     * Writes a record to the index file on page
     * Format: [number of records: 1 byte][data type: 1 byte][value: N bytes][array of rowIds: 4*len(rowIds) bytes]
     * @param value The value of the column this cell is for
     * @param rowIds The row ids that have this value
     * @param page The page to write to
     * @return The index where the cell was written to
     */
    public int writeCell(Object value, ArrayList<Integer> rowIds, int page, int childPage) throws IOException {
        this.seek((long) page * pageSize);
        Constants.PageType pageType = Constants.PageType.fromValue(this.readByte());
        // get cell size
        short payloadSize = (short) (2 + valueSize + 4 * rowIds.size());
        short cellSize = (short) (2 + payloadSize);
        if (valueSize == -1) {
            cellSize += ((String) value).length() + 1;
        }
        if (pageType == Constants.PageType.INDEX_INTERIOR) {
            cellSize += 4;
        }
        if (shouldSplit(page, cellSize)) {
            page = splitPage(page, value);
        }

        int insertionPoint = findValuePosition(value, page);
        int offset;
        if (insertionPoint == getNumberOfCells(page) - 1) {
            offset = getContentStart(page);
        } else {
            offset = shiftCells(page, insertionPoint, cellSize);
        }
        incrementNumberOfCells(page);

        // write cell start to cell pointer array
        this.seek((long) page * pageSize + 0x03 + 2L * insertionPoint);
        this.writeShort(offset);

        // write number of records
        this.seek((long) page * pageSize + offset);
        if (pageType == Constants.PageType.INDEX_LEAF) {
            this.writeInt(childPage);
        }
        this.writeByte(payloadSize);
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
        return insertionPoint + 1;
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
    public void addItemToCell(Object value, int rowId) throws IOException {
        // Find the cell to add the record to
        int[] pageAndIndex = findValue(value);
        int page = pageAndIndex[0];
        int index = pageAndIndex[1];
        int exists = pageAndIndex[2];
        // If the cell doesn't exist, create it and exit
        if (exists == 0) {
            writeCell(value, new ArrayList<>(Collections.singletonList(rowId)), page, -1);
            return;
        }
        if (shouldSplit(page, 4)) {
            page = splitPage(page, value);
            index = findValuePosition(value, page);
        }
        this.seek((long) page * pageSize);
        Constants.PageType pageType = Constants.PageType.fromValue(this.readByte());

        // Make space for the new row id
        int newOffset = this.shiftCells(page, index - 1, 4);

        this.seek((long) page * pageSize + newOffset + 2L);
        if (pageType == Constants.PageType.INDEX_INTERIOR) {
            this.skipBytes(4);
        }
        ArrayList<Integer> rowIds = new ArrayList<>();
        int numRowIds = this.readByte();
        int dataType = this.readByte();
        if (dataType >= 0x0C) {
            this.skipBytes(dataType - 0x0C);
        } else {
            this.skipBytes(valueSize);
        }
        long rowIdOffset = this.getFilePointer();
        for (int i = 0; i < numRowIds; i++) {
            int ri = this.readInt();
            if (rowId < ri && (i == 0 || rowId > rowIds.get(i - 1))) {
                rowIds.add(rowId);
            }
            rowIds.add(ri);
        }
        if (rowId > rowIds.get(rowIds.size() - 1)) {
            rowIds.add(rowId);
        }
        this.seek(rowIdOffset);
        for (int ri : rowIds) {
            this.writeInt(ri);
        }
    }

    /**
     * Finds the page and index of the cell containing value
     * @param value The value to search for
     * @return The page and index of the cell containing value and flag denoting if the cell exists
     * If the cell exists, the last element is 1
     * If the cell doesn't exist, the page and index mark the cell that should be before it
     * and the last element is 0
     */
    public int[] findValue(Object value) throws IOException {
        int currentPage = getRootPage();
        while (true) {
            this.seek((long) currentPage * pageSize);
            Constants.PageType pageType = Constants.PageType.fromValue(this.readByte());

            int index = findValuePosition(value, currentPage);
            int offset = getCellOffset(currentPage, index);
            Object cellValue = readValue(currentPage, offset);
            if (DataFunctions.compareTo(dataType, value, cellValue) == 0) {
                return new int[] {currentPage, index, 1};
            } else if (pageType == Constants.PageType.INDEX_LEAF) {
                return new int[] {currentPage, index, 0};
            } else {
                this.seek((long) currentPage * pageSize + offset);
                currentPage = this.readInt();
            }
        }

    }

    public ArrayList<Integer> search(String value, String operator) throws IOException {
        return null;
    }
}
