package MaxwellBase;

import Constants.Constants;

import java.io.IOException;
import java.util.ArrayList;

public class TableFile extends DatabaseFile{

    public TableFile(String tableName, String path) throws IOException {
        super(tableName + ".tbl", Constants.PageType.TABLE_LEAF, path);
    }


    /**
     * Find the rowid of the last record on a page
     * @param page the page to search
     * @return the rowid of the firts record on the page
     */
    public int getMinRowId(int page) throws IOException {
        if (getNumberOfCells(page) <= 0){
            throw new IOException("Page is empty");
        }
        Constants.PageType pageType = getPageType(page);
        int offset = getCellOffset(page, 0);
        this.seek((long) page * pageSize + offset);
        if (pageType == Constants.PageType.TABLE_LEAF) {
            this.skipBytes(2);
        } else if (pageType == Constants.PageType.TABLE_INTERIOR) {
            this.skipBytes(4);
        }
        return this.readInt();
    }

    public int getLastRowId() throws IOException {
        int lastPage = getLastLeafPage();
        int offset = getContentStart(lastPage);
        int numberOfCells = getNumberOfCells(lastPage);
        if (numberOfCells == 0){
            return -1;
        }
        Constants.PageType pageType = getPageType(lastPage);
        this.seek((long) lastPage * pageSize + offset);
        if (pageType == Constants.PageType.TABLE_LEAF) {
            this.skipBytes(2);
        } else if (pageType == Constants.PageType.TABLE_INTERIOR) {
            this.skipBytes(4);
        }
        return this.readInt();
    }


    /**
     * Split a page into two pages
     * No records are moved, this is a b+1 tree
     * If root page is split, a new root page is created
     * @param pageNumber the page to split
     * @return the page number of the new page
     */
    public int splitPage(int pageNumber, int splittingRowId) throws IOException {
        Constants.PageType pageType = getPageType(pageNumber);
        int parentPage = getParentPage(pageNumber);
        if (parentPage == 0xFFFFFFFF) {
            parentPage = createPage(0xFFFFFFFF, Constants.PageType.TABLE_INTERIOR);
            this.seek((long) pageNumber * pageSize + 0x0A);
            this.writeInt(parentPage);
            writePagePointer(parentPage, pageNumber, getMinRowId(pageNumber));
        }
        int newPage = createPage(parentPage, pageType);
        writePagePointer(parentPage, newPage, splittingRowId);
        if (pageType == Constants.PageType.TABLE_LEAF) {
            this.seek((long) pageNumber * pageSize + 0x06);
            this.writeInt(newPage);
        }
        return newPage;
    }

    /**
     * Write a page pointer to an interior page
     * Format: [page number][rowid]
     * @param page interior page to write to
     * @param pointer page number of the child page
     * @param rowId smallest rowid of the child page
     */
    public void writePagePointer(int page, int pointer, int rowId) throws IOException {
        short cellSize = 8;
        if (shouldSplit(page, cellSize)) {
            page = splitPage(page, rowId);
        }
        short contentStart = setContentStart(page, cellSize);
        int numCells = incrementNumberOfCells(page);
        this.seek((long) page * pageSize + 0x06);
        this.writeInt(pointer); // Write pointer to rightmost child
        this.seek((long) page * pageSize + 0x0E + numCells * 2);
        this.writeShort(contentStart); // Write to cell pointer array
        this.seek((long) page * pageSize + contentStart);
        this.writeInt(pointer); // Write cell
        this.writeInt(rowId);
    }

    public void writeRecord(Record record, int page) throws IOException {
        short recordSize = record.getRecordSize();
        short cellSize = (short) (recordSize + 6);
        if (shouldSplit(page, cellSize)) {
            page = splitPage(page, record.getRowId());
        }
        short contentStart = this.setContentStart(page, cellSize);
        short numberOfCells = incrementNumberOfCells(page);
        this.seek((long) pageSize * page + 0x0E + 2 * numberOfCells);
        this.writeShort(contentStart); // Write to cell pointer array
        this.seek((long) pageSize * page + contentStart);
        ArrayList<Constants.DataTypes> columns = record.getColumns();
        ArrayList<Object> values = record.getValues();
        this.writeShort(recordSize); // Write cell
        this.writeInt(record.getRowId());
        byte[] header = record.getHeader();
        this.write(header); // write record
        for (int i = 0; i < columns.size(); i++){
            writeData(columns.get(i), values.get(i));
        }
    }

    public void writeData(Constants.DataTypes type, Object value) throws IOException {
        switch (type) {
            case TINYINT, YEAR -> this.writeByte((byte) value);
            case SMALLINT -> this.writeShort((short) value);
            case INT, TIME -> this.writeInt((int) value);
            case BIGINT, DATE, DATETIME -> this.writeLong((long) value);
            case FLOAT -> this.writeFloat((float) value);
            case DOUBLE -> this.writeDouble((double) value);
            case TEXT -> this.writeBytes((String) value);
        }
    }
    public void appendRecord(Record record) throws IOException {
        int page = getLastLeafPage();
        writeRecord(record, page);
    }

    public void updateRecord(int rowId, int columnIndex, Object newValue) throws IOException {
        int[] pageAndIndex = findRecord(rowId);
        int page = pageAndIndex[0];
        int index = pageAndIndex[1];
        int exists = pageAndIndex[2];
        if (exists == 0) {
            throw new IOException("Record does not exist");
        }
        int offset = getCellOffset(page, index);
        Record record = readRecord(page, offset);
        if (record.getColumns().get(columnIndex) == Constants.DataTypes.TEXT) {
            int oldSize = ((String) record.getValues().get(columnIndex)).length();
            int newSize = ((String) newValue).length();
            if (shouldSplit(page, (short) (newSize - oldSize))) {
                splitPage(page, rowId);
                int[] newPageAndIndex = findRecord(rowId);
                page = newPageAndIndex[0];
                index = newPageAndIndex[1];
            }
            this.shiftCells(page, index -1, oldSize - newSize, 0);
            offset = getCellOffset(page, index);
        }
        ArrayList<Object> values = record.getValues();
        values.set(columnIndex, newValue);
        Record newRecord = new Record(record.getColumns(), values, record.getRowId());
        this.seek((long) page * pageSize + offset + 6);
        byte[] header = newRecord.getHeader();
        this.write(header);
        for (int i = 0; i < newRecord.getColumns().size(); i++){
            writeData(newRecord.getColumns().get(i), values.get(i));
        }
    }

    public void deleteRecord(int rowId) throws IOException {
        // TODO: Implement

    }

    /**
     * Delete all records that match the given value
     * @param columnIndex the column to search
     * @param value the value to search for
     * @param operator the operator to use
     * @return the number of records deleted
     */
    public int deleteRecords(int columnIndex, String value, String operator) throws IOException {
        // TODO: Implement
        return 0;
    }

    public int getLastLeafPage() throws IOException {
        int nextPage = getRootPage();
        while (true) {
            Constants.PageType pageType = getPageType(nextPage);
            if (pageType == Constants.PageType.TABLE_LEAF) {
                break;
            }
            this.seek((long) pageSize * nextPage + 0x06);
            nextPage = this.readInt();
        }
        return nextPage;
    }


    public Record getRecord(int rowId) throws IOException {
        int[] pageAndIndex = findRecord(rowId);
        int page = pageAndIndex[0];
        int index = pageAndIndex[1];
        int exists = pageAndIndex[2];
        if (exists == 0) {
            return null;
        }
        int offset = getCellOffset(page, index);
        return readRecord(page, offset);
    }

    public int[] findRecord(int rowId) throws IOException{
        int currentPage = getRootPage();
        while (true) {
            Constants.PageType pageType = getPageType(currentPage);
            int numCells = getNumberOfCells(currentPage);
            int currentCell = numCells / 2; // mid
            int low = 0; // L
            int high = numCells - 1; // R
            int currentRowId = getRowId(currentPage, currentCell);
            while (low < high /*currentRowId != rowId*/) {
                // binary search over cells
                // 0x10 location of cells in the page where each cell location = 2 byte
                if (currentRowId < rowId) {
                    low = currentCell; // may be inside the current cell so leave it
                } else if ( currentRowId > rowId) {
                    high = currentCell - 1; // - 1 must be left of the current cell
                } else {
                    break;
                }
                // currentCell = Math.ceil((double) (low + high) / 2);
                currentCell = (low + high + 1) / 2;
                currentRowId = getRowId(currentPage, currentCell);
            }
            if (pageType == Constants.PageType.TABLE_LEAF) {
                if (currentRowId == rowId) {
                    return new int[] {currentPage, currentCell, 1};
                } else {
                    return new int[] {currentPage, currentCell, 0};
                }
            } else if (pageType == Constants.PageType.TABLE_INTERIOR) {
                int offset = getCellOffset(currentPage, currentCell);
                this.seek((long) currentPage * pageSize + offset);
                currentPage = this.readInt();
            }
        }
    }

    private int getRowId(int page, int index) throws IOException {
        Constants.PageType pageType = getPageType(page);
        int offset = getCellOffset(page, index); //page offset: location of row as # of bytes from beg. of page
        this.seek((long) page * pageSize + offset);
        if (pageType == Constants.PageType.TABLE_INTERIOR) {
            this.skipBytes(4); // skip page pointer
        } else {
            this.skipBytes(2); // skip record size
        }
        return this.readInt();
    }

    /**
     * Read a record
     * @param page page number of the record
     * @param offset offset of the record on the page
     * @return Record object
     */
    private Record readRecord(int page, int offset) throws IOException {
        this.seek((long) page * pageSize + offset);
        this.readShort(); // Record size
        int rowId = this.readInt();
        byte numColumns = this.readByte();
        byte[] columns = new byte[numColumns];
        for (int i = 0; i < numColumns; i++) {
            columns[i] = this.readByte();
        }
        ArrayList<Object> values = new ArrayList<>();
        ArrayList<Constants.DataTypes> columnTypes = new ArrayList<>();
        for (byte b : columns) {
            Constants.DataTypes dataType;
            if (b > 0x0C){
                dataType = Constants.DataTypes.TEXT;
            }
            else {
                dataType = Constants.DataTypes.values()[b];
            }
            columnTypes.add(dataType);
            switch (dataType) {
                case TINYINT, YEAR -> values.add(this.readByte());
                case SMALLINT -> values.add(this.readShort());
                case INT, TIME -> values.add(this.readInt());
                case BIGINT, DATE, DATETIME -> values.add(this.readLong());
                case FLOAT -> values.add(this.readFloat());
                case DOUBLE -> values.add(this.readDouble());
                case TEXT -> {
                    int textLength = b - 0x0C;
                    byte[] text = new byte[textLength];
                    this.readFully(text);
                    values.add(new String(text));
                }
                case NULL -> values.add(null);
            }
        }
        return new Record(columnTypes, values, rowId);
    }

    /**
     * Search for all records that match the given condition
     * @param columnIndex index of the column to check
     * @param value value to compare against
     * @param operator type of comparison
     * @return List of records that match the condition
     */
    public ArrayList<Record> search(int columnIndex, String value, String operator) throws IOException {
        ArrayList<Record> records = new ArrayList<>();

        int currentPage = getRootPage();
        Constants.PageType pageType = getPageType(currentPage);
        // Go to first leaf page
        while (pageType != Constants.PageType.TABLE_LEAF) {
            int offset = getCellOffset(currentPage, 0);
            this.seek((long) currentPage * pageSize + offset);
            currentPage = this.readInt();
            pageType = getPageType(currentPage);
        }
        // Iterate over all records in the leaf pages
        while (currentPage != 0xFFFFFFFF) {
            this.seek((long) currentPage * pageSize);
            int numberOfCells = getNumberOfCells(currentPage);
            // Iterate over all records in the current page
            for (int i = 0; i < numberOfCells; i++) {
                this.seek((long) currentPage * pageSize + 0x10 + 2 * i);
                int currentOffset = getCellOffset(currentPage, i);
                Record record = readRecord(currentPage, currentOffset);
                if (record.compare(columnIndex, value, operator)) {
                    records.add(record);
                }
            }
            this.seek((long) currentPage * pageSize + 0x06);
            currentPage = this.readInt();
        }

        return records;
    }

}
