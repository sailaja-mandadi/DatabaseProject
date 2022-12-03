package MaxwellBase;

import Constants.Constants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

public class TableFile extends DatabaseFile{

    public TableFile(String tableName) throws IOException {
        super(tableName + ".tbl", Constants.PageType.TABLE_LEAF);
    }

    public static void main(String[] args) {
        try (TableFile tableFile = new TableFile("test.tbl")) {
            tableFile.seek(0);
            System.out.println("Page type: " + tableFile.readByte());
            tableFile.readByte();
            System.out.println("Number of records: " + tableFile.readShort());
            System.out.println("Start of content: " + tableFile.readShort());
            System.out.println("Right page pointer: " + tableFile.readInt());
            System.out.println("Parent page pointer: " + tableFile.readInt());
            ArrayList<Constants.DataTypes> testcol = new ArrayList<>();
            testcol.add(Constants.DataTypes.INT);
            testcol.add(Constants.DataTypes.TEXT);
            testcol.add(Constants.DataTypes.DOUBLE);
            for (int i = 0; i < 10; i++) {
                Random rand = new Random();
                ArrayList<Object> testrow = new ArrayList<>();
                System.out.println("Inserting row " + i);
                int col1 = rand.nextInt(100);
                System.out.println("\tcol1: " + col1);
                String col2 = "testajajajajajajajajajajajajajajajajajajajajajajajajaj" + (char)(rand.nextInt(26) + 'a');
                System.out.println("\tcol2: " + col2);
                double col3 = rand.nextDouble();
                System.out.println("\tcol3: " + col3);
                testrow.add(col1);
                testrow.add(col2);
                testrow.add(col3);
                Record record = new Record(testcol, testrow, i);
                System.out.println("\tRecord size: " + record.getRecordSize());
                tableFile.appendRecord(record);
            }
            int numRecords = tableFile.getNumberOfCells(0);
            int page = 0;
            for (int i = 0; i < numRecords; i++) {
                tableFile.seek(i * 2 + 0x10);
                int offset = tableFile.readShort();
                Record record = tableFile.readRecord(page, offset);
                System.out.println(record);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * Find the rowid of the last record on a page
     * @param page the page to search
     * @return the rowid of the last record on the page
     */
    public int getMaxRowId(int page) throws IOException {
        if (getNumberOfCells(page) <= 0){
            throw new IOException("Page is empty");
        }
        this.seek((long) page * pageSize);
        Constants.PageType pageType = Constants.PageType.fromValue(this.readByte());
        short contentStart = getContentStart(page);
        this.seek((long) page * pageSize + contentStart);
        if (pageType == Constants.PageType.TABLE_LEAF) {
            this.skipBytes(2);
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
    public int splitPage(int pageNumber) throws IOException {
        this.seek((long) pageNumber * pageSize);
        byte pageTypeByte = this.readByte();
        Constants.PageType pageType = Constants.PageType.fromValue(pageTypeByte);
        int parentPage = getParentPage(pageNumber);
        if (parentPage == 0xFFFFFFFF) {
            parentPage = createPage(0xFFFFFFFF, Constants.PageType.TABLE_INTERIOR);
            this.seek((long) pageNumber * pageSize + 0x0A);
            this.writeInt(parentPage);
        }
        int newPage = createPage(parentPage, pageType);
        writePagePointer(parentPage, newPage, getMaxRowId(pageNumber) + 1);
        this.skipBytes(5);
        if (pageType == Constants.PageType.TABLE_LEAF) {
            this.writeInt(newPage);
        }
        return newPage;
    }

    /**
     * Write a page pointer to an interior page
     * @param page interior page to write to
     * @param pointer page number of the child page
     * @param rowId smallest rowid of the child page
     */
    public void writePagePointer(int page, int pointer, int rowId) throws IOException {
        short cellSize = 8;
        if (!canFit(page, cellSize)) {
            page = splitPage(page);
        }
        short contentStart = setContentStart(page, cellSize);
        int numCells = getNumberOfCells(page);
        this.seek((long) page * pageSize + 0x06 + numCells * 2);
        this.writeInt(contentStart);
        this.seek((long) page * pageSize + contentStart);
        this.writeInt(rowId);
        this.writeInt(pointer);
    }

    public void writeRecord(Record record, int page) throws IOException {
        short recordSize = record.getRecordSize();
        short cellSize = (short) (recordSize + 6);
        if (!canFit(page, cellSize)) {
            page = splitPage(page);
        }
        short contentStart = this.setContentStart(page, cellSize);
        short numberOfCells = incrementNumberOfCells(page);
        this.seek((long) pageSize * page + 0x0E + 2 * numberOfCells);
        this.writeShort(contentStart);
        this.seek((long) pageSize * page + contentStart);
        ArrayList<Constants.DataTypes> columns = record.getColumns();
        ArrayList<Object> values = record.getValues();
        this.writeShort(recordSize);
        this.writeInt(record.getRowId());
        byte[] header = record.getHeader();
        this.write(header);
        for (int i = 0; i < columns.size(); i++){
            switch (columns.get(i)) {
                case TINYINT, YEAR -> this.writeByte((byte) values.get(i));
                case SMALLINT -> this.writeShort((short) values.get(i));
                case INT, TIME -> this.writeInt((int) values.get(i));
                case BIGINT, DATE, DATETIME -> this.writeLong((long) values.get(i));
                case FLOAT -> this.writeFloat((float) values.get(i));
                case DOUBLE -> this.writeDouble((double) values.get(i));
                case TEXT -> this.writeBytes((String) values.get(i));
            }
        }
    }

    private void appendRecord(Record record) throws IOException {
        Constants.PageType pageType;
        int nextPage = (int) (this.length() / pageSize) - 1;
        while (true) {
            this.seek((long) pageSize * nextPage);
            pageType = Constants.PageType.fromValue(this.readByte());
            if (pageType == Constants.PageType.TABLE_LEAF) {
                break;
            }
            this.seek((long) pageSize * nextPage + 0x06);
            nextPage = this.readInt();
        }
        System.out.println("Writing record to page " + nextPage);
        writeRecord(record, nextPage);
    }

    public Record getRecord(int rowId) throws IOException {
        int rootPage = getRootPage();
        this.seek((long) rootPage * pageSize);
        Constants.PageType pageType = Constants.PageType.fromValue(this.readByte());



        return
    }

    public int findRecord(int rowId) throws IOException{
        int currentPage = getRootPage();
        while (true) {
            this.seek((long) currentPage * pageSize);
            Constants.PageType pageType = Constants.PageType.fromValue(this.readByte());
            int numCells = getNumberOfCells(currentPage);
            int currentCell = numCells / 2; // mid
            int low = 0; // L
            int high = numCells - 1; // R
            int currentRowId = -1;
            int lastRowId = -1;
            while (low <= high /*currentRowId != rowId*/) {
                // binary search over cells
                // 0x10 location of cells in the page where each cell location = 2 byte
                this.seek((long) currentPage * pageSize + 0x10 + 2 * currentCell);
                int offset = this.readShort(); //page offset: location of row as # of bytes from beg. of page
                this.seek((long) currentPage * pageSize + offset);
                if (pageType == Constants.PageType.TABLE_INTERIOR) {
                    this.skipBytes(2);
                    lastRowId = currentRowId;
                }
                currentRowId = this.readInt();

                //currentRowId = math.floor((low + high) / 2);
                if (Arr[currentRowId] < rowId){
                    low = currentRowId + 1; // + 1 as to the right
                }
                else if ( Arr[currentRowId] > rowId)
                {
                    high = currentRowId - 1; // - 1 as in one cell to the left
                }
                else {
                    currentRowId = rowId;
                }
            }
            // currentRowId stays -1, meaning row is not found.
        }


        return
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
        this.seek((long) currentPage * pageSize);
        Constants.PageType pageType = Constants.PageType.fromValue(this.readByte());
        // Go to first leaf page
        while (pageType != Constants.PageType.TABLE_LEAF) {
            this.seek((long) currentPage * pageSize);
            pageType = Constants.PageType.fromValue(this.readByte());
            int contentStart = getContentStart(currentPage);
            this.seek((long) currentPage * pageSize + contentStart + 2);
            currentPage = this.readInt();
        }
        // Iterate over all records in the leaf pages
        while (currentPage != 0xFFFFFFFF) {
            this.seek((long) currentPage * pageSize);
            int numberOfCells = getNumberOfCells(currentPage);
            // Iterate over all records in the current page
            for (int i = 0; i < numberOfCells; i++) {
                this.seek((long) currentPage * pageSize + 0x10 + 2 * i);
                int currentOffset = this.readInt();
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
