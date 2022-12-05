package MaxwellBase;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import Constants.Constants;

public class Test {
//    public static void main(String[] args) {
//        try {
//            File tableFile = new File("test.tbl");
//            if (tableFile.exists()) {
//                tableFile.delete();
//            }
//        } catch (SecurityException e) {
//            System.out.println("Unable to delete file");
//        }
//        try (TableFile tableFile = new TableFile("test")) {
//            int[] pageInfo = tableFile.getPageInfo(0);
//            System.out.println("Page 0: ");
//            System.out.println("\tPage Type: " + Constants.PageType.fromValue(pageInfo[0]));
//            System.out.println("\tNumber of Records: " + pageInfo[1]);
//            System.out.println("\tContent start: " + pageInfo[2]);
//            System.out.println("\tNext Page: " + pageInfo[3]);
//            System.out.println("\tParent Page: " + pageInfo[4]);
//            ArrayList<Constants.DataTypes> testcol = new ArrayList<>();
//            testcol.add(Constants.DataTypes.INT);
//            testcol.add(Constants.DataTypes.TEXT);
//            testcol.add(Constants.DataTypes.DOUBLE);
////            var records = generateRanomRecords(testcol, 10);
//            int i = 0;
//            for (var record : records) {
//                System.out.println("Inserting row " + i);
//                i++;
//                tableFile.appendRecord(record);
//            }
//            var search_result = tableFile.search(-1, null, null);
//            for (var record : search_result) {
//                System.out.println(record);
//            }
//
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }

}
