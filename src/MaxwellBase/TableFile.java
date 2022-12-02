package MaxwellBase;

import Constants.Constants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

public class TableFile extends DatabaseFile{
    String tableName;
    public TableFile(String name) throws IOException {
        super(name);
        this.tableName = name;
        this.fileType = Constants.FileType.TABLE;
        createPage(0xFFFFFFFF, Constants.PageType.TABLE_LEAF);
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

    public void writePagePointer(int page, int pointer, int rowId) throws IOException {
        short cellSize = 8;
        if (!canFit(page, cellSize)) {
            page = splitPage(page);
        }
        short contentStart = setContentStart(page, cellSize);
        this.seek((long) page * pageSize + 0x06);
        this.writeInt(pointer);
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

    public ArrayList<Record> search(int columnIndex, String value, String operator) throws IOException {
        ArrayList<Record> records = new ArrayList<>();

        int currentPage = 0;
        while (true) {
            this.seek((long) currentPage * pageSize);
            Constants.PageType pageType = Constants.PageType.fromValue(this.readByte());
            if (pageType == Constants.PageType.TABLE_LEAF) {
                int numberOfCells = getNumberOfCells(currentPage);
                int contentStart = getContentStart(currentPage);
                int currentOffset = contentStart;
                for (int i = 0; i < numberOfCells; i++) {
                    this.seek((long) currentPage * pageSize + currentOffset);
                    Record record = readRecord(currentPage, currentOffset);
                    if (record.compare(columnIndex, value, operator)) {
                        records.add(record);
                    }
                    currentOffset += record.getRecordSize() + 6;
                }
            }
            this.seek((long) currentPage * pageSize + 0x06);
            currentPage = this.readInt();
        }

        return records;
    }

}
