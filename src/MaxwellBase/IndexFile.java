package MaxwellBase;

import Constants.Constants;

import java.io.IOException;
import java.util.*;

public class IndexFile extends DatabaseFile{
    Constants.DataTypes dataType;
    short valueSize;
    String tableName;
    int columnIndex;
    String path;

    /**
     * Creates a new IndexFile object
     * @param table The table that the index file is for
     * @param columnName The column that the index file indexes
     */
    public IndexFile(Table table, String columnName, String path) throws IOException {
        super(table.tableName + "." + columnName + ".ndx", Constants.PageType.INDEX_LEAF, path);
        this.tableName = table.tableName;
        this.columnIndex = table.columnNames.indexOf(columnName);
        this.path = path;
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
        Constants.PageType pageType = getPageType(page);
        this.seek((long) page * pageSize + offset);
        if (pageType == Constants.PageType.INDEX_INTERIOR) {
            this.skipBytes(4);
        }
        int payloadSize = this.readShort();
        if (payloadSize == 0) {
            return null;
        }
        this.skipBytes(1);
        byte recordType = this.readByte();
        if (recordType == 0) {
            return null;
        }
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
        Constants.PageType pageType = getPageType(pageNumber);

        int parentPage = getParentPage(pageNumber);
        if (parentPage == 0xFFFFFFFF) {
            parentPage = createPage(0xFFFFFFFF, Constants.PageType.INDEX_INTERIOR);
            this.seek((long) parentPage * pageSize + 0x10);
            this.writeShort(pageSize - 6);
            this.seek((long) (parentPage + 1) * pageSize - 6);
            // Pointer to leftmost page has no corresponding cell payload
            this.writeInt(pageNumber);
            this.writeByte(0);

            this.seek((long) parentPage * pageSize + 0x02);
            this.writeShort(1);
            this.writeShort(pageSize - 6);

            // Set parent to 0xFFFFFFFF
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
        this.writeCell(middleRecordValue, middleRecordPointers, parentPage, newPage);

        // Fill space where the middle record was with 0s
        this.seek((long) pageNumber * pageSize + middleRecordOffset);
        int cellSize = payloadSize + 2 + (pageType == Constants.PageType.INDEX_INTERIOR ? 4: 0);
        for (int i = 0; i < cellSize; i++) {
            this.writeByte(0);
        }

        // Move the cells after the middle record to the new page
        this.moveCells(pageNumber, newPage, middleRecord);

        // Overwrite the offset of the middle record in old page's array of offsets
        this.seek((long) pageNumber * pageSize + 0x10 + middleRecord * 2);
        this.writeShort(0);

        int remainingCellOffset = getCellOffset(pageNumber, middleRecord - 1);
        this.seek((long) pageNumber * pageSize + 0x02);
        // Update the number of cells in the original page
        this.writeShort(middleRecord);
        // Update the contentStart pointer in the original page
        this.writeShort(remainingCellOffset);


        if (DataFunctions.compareTo(dataType, splittingValue, middleRecordValue) > 0) {
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
     * @param newRecord number of records to add or remove
     *                  positive number to add records
     *                  negative number to remove records
     *                  zero to not change the number of records
     * @return The page offset of the free space created
     */
    public int shiftCells(int page, int precedingCell, int shift, int newRecord) throws IOException {
        if (shouldSplit(page, shift)) {
            throw new IOException("Asked to shift cells more than the page can hold");
        }

        int oldContentStart = getContentStart(page);
        int contentOffset = setContentStart(page, (short) shift);
        if (contentOffset == pageSize) {
            return pageSize - shift;
        }
        int startOffset;
        if (precedingCell >= 0) {
            startOffset = getCellOffset(page, precedingCell);
        } else {
            startOffset = pageSize;
        }
        this.seek((long) page * pageSize + oldContentStart);
        int bytesToMove = startOffset - oldContentStart;
        if (shift < 0) {
            bytesToMove += shift;
        }
        byte[] shiftedBytes = new byte[bytesToMove];
        this.read(shiftedBytes);

        this.seek((long) page * pageSize + contentOffset);
        this.write(shiftedBytes);

        int numberOfCells = getNumberOfCells(page);
        // Update the offsets of the cells that were shifted
        this.seek((long) page * pageSize + 0x10 + (precedingCell + 1) * 2L);
        byte[] oldOffsets = new byte[(numberOfCells - precedingCell - 1) * 2];
        this.read(oldOffsets);

        // Shift the offsets by shift bytes
        // If we are adding a new record, leave room for it's offset
        // If we are removing a record, remove it's offset
        // If we are not changing the number of records, don't change the offsets
        this.seek((long) page * pageSize + 0x10 + (precedingCell + 1 + newRecord) * 2L);

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
        if (numberOfCells == 0) {
            return -1;
        }
        int mid = numberOfCells / 2;
        int low = -1;
        int high = numberOfCells - 1;
        int currentOffset = getCellOffset(page, mid);
        while (low < high) {
            Object currentValue = readValue(page, currentOffset);
            int comparison = DataFunctions.compareTo(dataType, currentValue, value);
            if (currentValue == null) {
                comparison = -1;
            }
            if (comparison == 0) {
                return mid;
            } else if (comparison < 0) { // currentValue < value
                low = mid;
            } else { // currentValue > value
                high = mid - 1;
            }
            mid = (int) Math.floor((float) (low + high + 1) / 2f);
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
        int numberOfCells = getNumberOfCells(sourcePage);
        int numberOfCellsToMove = numberOfCells - precedingCell - 1;
        int contentStart = getContentStart(sourcePage);

        // Read the bytes to be moved
        byte[] cellBytes = new byte[cellOffset - contentStart];
        this.seek((long) sourcePage * pageSize + contentStart);
        this.read(cellBytes);

        // Read the offsets of the cells to be moved
        byte[] cellOffsets = new byte[(getNumberOfCells(sourcePage) - precedingCell - 1) * 2];
        this.seek((long) sourcePage * pageSize + 0x10 + (precedingCell + 1) * 2L);
        this.read(cellOffsets);

        // Overwrite the old offsets with 0s
        this.seek((long) sourcePage * pageSize + 0x10 + (precedingCell + 1) * 2L);
        byte[] zeros = new byte[cellOffsets.length];
        this.write(zeros);

        // Write the bytes to be moved
        int newContentStart = getContentStart(destinationPage) - (cellBytes.length);
        this.seek((long) destinationPage * pageSize + newContentStart);
        this.write(cellBytes);

        // Write the new contentStart pointer in the destination page
        this.seek((long) destinationPage * pageSize + 0x04);
        this.writeShort(newContentStart);

        // Write the offsets of the cells to be moved
        int offsetDiff = contentStart - newContentStart;
        this.seek((long) destinationPage * pageSize + 0x10);
        for (int i = 0; i < cellOffsets.length; i += 2) {
            short offset = (short) ((cellOffsets[i] << 8) | (cellOffsets[i + 1] & 0xFF));
            this.writeShort(offset - offsetDiff);
        }

        //write the number of cells in the destination page
        this.seek((long) destinationPage * pageSize + 0x02);
        this.writeShort(numberOfCellsToMove);


        // Fill the space left by the moved cells with 0s
        zeros = new byte[cellOffset - contentStart];
        this.seek((long) sourcePage * pageSize + contentStart);
        this.write(zeros);
    }


    /**
     * Initializes the index file with all the records in the table
     */
    public void initializeIndex() throws IOException {
        try (TableFile table = new TableFile(tableName, path)) {
            ArrayList<Record> records = table.search(-1, null, null); // Get all records
            Set<Object> values = new HashSet<>();
            Map<Object, ArrayList<Integer>> valueToRowId = new HashMap<>();
            for (Record record : records) {
                Object value = record.getValues().get(this.columnIndex);
                if (value == null) {
                    continue;
                }
                if (!values.contains(value)) {
                    values.add(value);
                    if (!valueToRowId.containsKey(value)) {
                        valueToRowId.put(value, new ArrayList<>());
                    }
                    valueToRowId.get(value).add(record.getRowId());
                }
            }
            Object[] sortedValues = values.toArray();
            Arrays.sort(sortedValues, (o1, o2) -> DataFunctions.compareTo(dataType, o1, o2));
            for (Object value : sortedValues) {
                int[] pageAndIndex = this.findValue(value);
                int page = pageAndIndex[0];
                this.writeCell(value, valueToRowId.get(value), page, -1);
            }
        }
    }
    /**
     * Writes a record to the index file on page
     * Format: [number of records: 1 byte][data type: 1 byte][value: N bytes][array of rowIds: 4*len(rowIds) bytes]
     * @param value The value of the column this cell is for
     * @param rowIds The row ids that have this value
     * @param page The page to write to
     */
    public void writeCell(Object value, ArrayList<Integer> rowIds, int page, int childPage) throws IOException {
        Constants.PageType pageType = getPageType(page);

        // get cell size
        short payloadSize = (short) (2 + valueSize + 4 * rowIds.size());
        if (valueSize == -1) {
            payloadSize += ((String) value).length() + 1;
        }
        short cellSize = (short) (2 + payloadSize);
        if (pageType == Constants.PageType.INDEX_INTERIOR) {
            cellSize += 4;
        }
        if (shouldSplit(page, cellSize)) {
            page = splitPage(page, value);
        }

        int insertionPoint = findValuePosition(value, page);
        int offset;
        if (insertionPoint == getNumberOfCells(page) - 1) {
            offset = setContentStart(page, cellSize);
        } else {
            offset = shiftCells(page, insertionPoint, cellSize, 1);
        }
        incrementNumberOfCells(page);

        // write cell start to cell pointer array
        this.seek((long) page * pageSize + 0x10 + 2L * (insertionPoint + 1));
        this.writeShort(offset);

        // write number of records
        this.seek((long) page * pageSize + offset);
        if (pageType == Constants.PageType.INDEX_INTERIOR) {
            if (childPage == -1) {
                throw new IOException("Child page not specified");
            }
            this.writeInt(childPage);
        }
        this.writeShort(payloadSize);
        this.writeByte(rowIds.size());
        // write data type
        if (dataType.ordinal() == 0x0C){
            this.writeByte(((String) value).length() + 0x0C);
        } else {
            this.writeByte(dataType.ordinal());
        }
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
        removeItemFromCell(oldValue, rowId);
        addItemToCell(newValue, rowId);
    }

    /**
     * Remove a record from a cell
     * @param value The value of the cell to remove the record from
     * @param rowId The row id of the record to remove
     */
    public void removeItemFromCell(Object value, int rowId) throws IOException {
        int[] pageAndIndex = this.findValue(value);
        int page = pageAndIndex[0];
        int index = pageAndIndex[1];
        int exists = pageAndIndex[2];
        if (exists == 0) {
            throw new IllegalArgumentException("Record does not exist");
        }
        removeItemFromCell(page, index, rowId);
    }

    /**
     * Remove a record from a cell
     * @param page The page of the cell to remove the record from
     * @param index The index of the cell to remove the record from
     * @param rowId The row id of the record to remove
     */
    public void removeItemFromCell(int page, int index, int rowId) throws IOException {
        int offset = getCellOffset(page, index);
        ArrayList<Integer> rowIds = this.readRowIds(page, offset);
        // Verify that the row id exists in the cell
        if (!rowIds.contains(rowId)) {
            throw new IllegalArgumentException("Row id not present in cell");
        }
        this.seek((long) page * pageSize + offset);
        // TODO: This
        rowIds.remove((Integer) rowId);
        // Remove the row id from the cell
        // use shiftCells remove space
        // Remove the row id from the cell if it is not empty
        Constants.PageType pageType = getPageType(page);
        this.seek((long) page * pageSize + offset);
        if (pageType == Constants.PageType.INDEX_INTERIOR) {
            this.skipBytes(4);
        }
        int payloadSize = this.readShort();
        this.writeByte(rowIds.size());
        this.skipBytes(payloadSize - 4 * (rowIds.size() + 1) - 1);
        for (int i : rowIds) {
            this.writeInt(i);
        }
        this.shiftCells(page, index - 1, -4, 0);

        if (rowIds.size() == 0) {
            // Delete the cell if it is empty
            this.deleteCell(page, index);
        }
    }

    /**
     * Delete a cell from the index file
     * Must have no row ids associated with it
     * @param page The page the cell is on
     * @param index The index of the cell
     */
    public void deleteCell(int page, int index) throws IOException {
        int offset = getCellOffset(page, index);
        ArrayList<Integer> rowIds = this.readRowIds(page, offset);

        if (rowIds.size() != 0) {
            throw new IllegalArgumentException("Cannot delete cell with row ids");
        }
        // TODO
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
        Constants.PageType pageType = getPageType(page);

        // Make space for the new row id
        this.shiftCells(page, index - 1, 4, 0);

        int newOffset = getCellOffset(page, index);

        ArrayList<Integer> rowIds = readRowIds(page, newOffset);
        rowIds.add(rowId);
        Collections.sort(rowIds);

        this.seek((long) page * pageSize + newOffset);
        if (pageType == Constants.PageType.INDEX_INTERIOR) {
            this.skipBytes(4);
        }
        // Update the payload size
        int payloadSize = this.readShort();
        this.seek((long) page * pageSize + newOffset);
        if (pageType == Constants.PageType.INDEX_INTERIOR) {
            this.skipBytes(4);
        }
        this.writeShort(payloadSize + 4);

        // Update the number of row ids
        this.writeByte(rowIds.size());

        int dataType = this.readByte();
        if (dataType >= 0x0C) {
            this.skipBytes(dataType - 0x0C);
        } else {
            this.skipBytes(valueSize);
        }
        // Write the row ids
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
            Constants.PageType pageType = getPageType(currentPage);
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

    public ArrayList<Integer> readRowIds(int page, int offset) throws IOException {
        Constants.PageType pageType = getPageType(page);
        this.seek((long) page * pageSize + offset);
        if (pageType == Constants.PageType.INDEX_INTERIOR) {
            this.skipBytes(4);
        }
        ArrayList<Integer> rowIds = new ArrayList<>();
        int payloadSize = this.readShort();
        if (payloadSize == 0) {
            return new ArrayList<>();
        }
        int numRowIds = this.readByte();
        int dataType = this.readByte();
        if (dataType >= 0x0C) {
            this.skipBytes(dataType - 0x0C);
        } else {
            this.skipBytes(valueSize);
        }
        for (int i = 0; i < numRowIds; i++) {
            rowIds.add(this.readInt());
        }
        return rowIds;
    }

    public ArrayList<Integer> traverse(int page, int start, int end, int direction) throws IOException {
        if (start > end) {
            return new ArrayList<>();
        }
        ArrayList<Integer> rowIds = new ArrayList<>();
        Constants.PageType pageType = getPageType(page);
        int currentCell = start;
        int offset = getCellOffset(page, currentCell);
        while (currentCell <= end) {
            offset = getCellOffset(page, currentCell);
            rowIds.addAll(readRowIds(page, offset));
            if (pageType == Constants.PageType.INDEX_INTERIOR) {
                this.seek((long) page * pageSize + offset);
                int nextPage = this.readInt();
                rowIds.addAll(traverse(nextPage, 0, getNumberOfCells(nextPage) - 1, direction));
            }
            currentCell++;
        }
        int parentPage = getParentPage(page);
        if (parentPage == -1) {
            return rowIds;
        }

        int parentIndex = findValuePosition(readValue(page, offset), parentPage);
        if (direction == -1) {
            rowIds.addAll(traverse(parentPage, 0, parentIndex - 1, direction));
            rowIds.addAll(readRowIds(parentPage, getCellOffset(parentPage, parentIndex)));
        } else if (direction == 1) {
            rowIds.addAll(traverse(parentPage, parentIndex + 1, getNumberOfCells(parentPage) - 1, direction));
        } else if (direction == 0) {
            rowIds.addAll(traverse(parentPage, 0, getNumberOfCells(parentPage) - 1, direction));
        } else {
            throw new IllegalArgumentException("Direction must be -1, 0, or 1");
        }
        return rowIds;
    }

    public ArrayList<Integer> search(String value, String operator) throws IOException {
        ArrayList<Integer> rowIds;
        int[] pageAndIndex = findValue(value);

        int page = pageAndIndex[0];
        int index = pageAndIndex[1];
        int exists = pageAndIndex[2];
        int offset = getCellOffset(page, index);
        rowIds = switch (operator) {
            case "=" -> exists == 1 ? readRowIds(page, offset) : new ArrayList<>();
            case "<>" -> {
                var temp = traverse(page, 0, index - 1, -1);
                temp.addAll(traverse(page, index + 1, getNumberOfCells(page) - 1, 1));
                yield temp;
            }
            case "<" -> traverse(page, 0, index - 1, -1);
            case "<=" -> traverse(page, 0, index, -1);
            case ">" -> traverse(page, index + 1, getNumberOfCells(page) - 1, 1);
            case ">=" ->traverse(page, index, getNumberOfCells(page) - 1, 1);
            default -> throw new IllegalArgumentException("Operator must be =, <>, <, <=, >, or >=");
        };



        return rowIds;
    }
}
