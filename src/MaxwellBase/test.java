package MaxwellBase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import Constants.Constants;

public class test {
    public static void main(String[] args) {
        try (TableFile tableFile = new TableFile("test",Settings.getUserDataDirectory())) {
            int[] pageInfo = tableFile.getPageInfo(0);
            System.out.println("Page 0: ");
            System.out.println("\tPage Type: " + pageInfo[0]);
            System.out.println("\tNumber of Records: " + pageInfo[1]);
            System.out.println("\tContent start: " + pageInfo[2]);
            System.out.println("\tNext Page: " + pageInfo[3]);
            System.out.println("\tParent Page: " + pageInfo[4]);
            ArrayList<Constants.DataTypes> testcol = new ArrayList<>();
            testcol.add(Constants.DataTypes.INT);
            testcol.add(Constants.DataTypes.TEXT);
            testcol.add(Constants.DataTypes.DOUBLE);
            var records = generateRanomRecords(testcol, 20);
            for (var record : records) {
                tableFile.appendRecord(record);
            }
            var search_result = tableFile.search(-1, null, null);
            for (var record : search_result) {
                System.out.println(record);
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static ArrayList<Record> generateRanomRecords(ArrayList<Constants.DataTypes> cols, int num) {
        ArrayList<Record> records = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Random rand = new Random();
            ArrayList<Object> values = new ArrayList<>();
            System.out.println("Inserting row " + i);
            int col1 = rand.nextInt(100);
            System.out.println("\tcol1: " + col1);
            String col2 = "test (";
            for (int c = 0; c < 20; c++) {
                col2 += (char)(rand.nextInt(26) + 'a');
            }
            col2 += ")";
            System.out.println("\tcol2: " + col2);
            double col3 = rand.nextDouble();
            System.out.println("\tcol3: " + col3);
            values.add(col1);
            values.add(col2);
            values.add(col3);
            Record record = new Record(cols, values, i);
            records.add(record);
        }
        return records;
    }
}
