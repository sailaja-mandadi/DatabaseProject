package MaxwellBase;

import Constants.Constants;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

import static org.junit.jupiter.api.Assertions.*;

public class TableFileTest {

    public TableFile tableFile;
    public ArrayList<Constants.DataTypes> columnTypes;
    public ArrayList<Record> records;

    @Before
    public void setUp(){
        columnTypes = new ArrayList<>();
        columnTypes.add(Constants.DataTypes.TEXT);
        columnTypes.add(Constants.DataTypes.TEXT);
        columnTypes.add(Constants.DataTypes.INT);
        columnTypes.add(Constants.DataTypes.TEXT);
        Scanner sc;
        try {
            File tFile = new File("data/user_data/test.tbl");
            if (tFile.exists()) {
                if (!tFile.delete()){
                    throw new Exception("Could not delete table file");
                }
            }
            tableFile = new TableFile("test", "data/user_data");
            records = new ArrayList<>();
            File data_file = new File("Test/testdata.txt");
            sc = new Scanner(data_file);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        int i = 0;
        while (sc.hasNextLine()) {
            String line = sc.nextLine();
            String[] values = line.split("; ");
            int col2 = Integer.parseInt(values[2]);
            ArrayList<Object> recordValues = new ArrayList<>();
            recordValues.add(values[0]);
            recordValues.add(values[1]);
            recordValues.add(col2);
            recordValues.add(values[3]);
            Record record = new Record(columnTypes, recordValues, i);
            records.add(record);
            i++;
        }
    }

    @Test
    public void getMinRowId() {
        appendRecord();
        try {
            assertEquals(0, tableFile.getMinRowId(0));
            assertEquals(0, tableFile.getMinRowId(1));
            assertEquals(4, tableFile.getMinRowId(2));
            assertEquals(8, tableFile.getMinRowId(3));

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void splitPage() {
        try {
            for (int i = 0; i < 3; i++) {
                tableFile.appendRecord(records.get(i));
            }
            int result  = tableFile.splitPage(0, 4);
            assertEquals(2, result);
            assertEquals(0, tableFile.getMinRowId(1));

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void writePagePointer() {
        try {
            tableFile.createPage(0, Constants.PageType.TABLE_INTERIOR);
            tableFile.writePagePointer(1, 0, 4);
            assertEquals(1024, tableFile.length());
            assertEquals(4, tableFile.getMinRowId(1));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void writeRecord() {
        try {
            tableFile.writeRecord(records.get(0), 0);
            int cellSize = records.get(0).getRecordSize() + 6;
            assertEquals(512, tableFile.length());
            int[] pageInfo = tableFile.getPageInfo(0);
            assertEquals(Constants.PageType.TABLE_LEAF, Constants.PageType.fromValue(pageInfo[0]));
            assertEquals(1, pageInfo[1]);
            assertEquals(512 - cellSize, pageInfo[2]);
            assertEquals(-1, pageInfo[3]);
            assertEquals(-1, pageInfo[4]);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void appendRecord() {
        try {
            for (Record record : records) {
                    tableFile.appendRecord(record);
            }
            assertEquals(2048, tableFile.length());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Test
    public void getLastLeafPage() {
        appendRecord();
        try {
            assertEquals(3, tableFile.getLastLeafPage());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void getRecord() {
        appendRecord();
        try {
            for (int i = 0; i < records.size(); i++) {
                Record record = tableFile.getRecord(i);
                assertEquals(records.get(i).getValues(), record.getValues());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void search() {
        appendRecord();
        ArrayList<Record> result = new ArrayList<>();
        for (Record record : records) {
            if ((int) record.getValues().get(2) > 8000) {
                result.add(record);
            }
        }
        try {
            ArrayList<Record> searchResult = tableFile.search(2, "8000", ">");
            assertEquals(result, searchResult);
            for (int i = 0; i < result.size(); i++) {
                assertEquals(result.get(i), searchResult.get(i));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void shiftCells() {
        appendRecord();
        try {
            int originalOffset = tableFile.getCellOffset(3, 1);
            tableFile.shiftCells(3, 0, 50, 0);
            assertEquals(originalOffset - 50, tableFile.getCellOffset(3, 1));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void updateRecord() {
        appendRecord();
        try {
            tableFile.updateRecord(9, 1, "TEST UPDATE RECORD ABC DEF GHI JKL MNO PQR STU VWX YZ");
            Record updatedRecord = tableFile.getRecord(9);
            assertEquals("TEST UPDATE RECORD ABC DEF GHI JKL MNO PQR STU VWX YZ", updatedRecord.getValues().get(1));
            tableFile.updateRecord(1, 2, 9999);
            updatedRecord = tableFile.getRecord(1);
            assertEquals(9999, updatedRecord.getValues().get(2));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void deleteRecord() {
        appendRecord();
        try {
            tableFile.deleteRecord(2);
            Record deletedRecord = tableFile.getRecord(2);
            assertNull(deletedRecord);
            var searchResult = tableFile.search(2, "9800", "=");
            assertEquals(0, searchResult.size());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}