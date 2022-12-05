package MaxwellBase;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

public class IndexFileTest {

    IndexFile indexFile;
    TableFile tableFile;

    @Before
    public void setUp() throws Exception {
        TableFileTest tableFileTest = new TableFileTest();
        tableFileTest.setUp();
        tableFileTest.appendRecord();
        ArrayList<String> columnNames = new ArrayList<>();
        ArrayList<Boolean> nullable = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            columnNames.add("col" + i);
            nullable.add(true);
        }
        Table table = new Table("test", columnNames, tableFileTest.columnTypes, nullable, true);
        try {
            File iFile = new File("data/user_data/test.col1.ndx");
            if (iFile.exists()) {
                if (!iFile.delete()){
                    throw new Exception("Could not delete index file");
                }
            }
            indexFile = new IndexFile(table, "col1", "data/user_data");
            tableFile = tableFileTest.tableFile;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void readValue() {
    }

    @Test
    public void splitPage() {
    }

    @Test
    public void shiftCells() {
    }

    @Test
    public void findValuePosition() {
    }

    @Test
    public void moveCells() {
    }

    @Test
    public void initializeIndex() {
        try {
            indexFile.initializeIndex();
            assert indexFile.length() == 1536;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void writeCell() {
    }

    @Test
    public void update() {
        initializeIndex();
        try {
            indexFile.update(9, "Terminology St 176, Summerholm, Guadeloupe, 673843", "2345 Test St, Testville, Testland, 12345");
            ArrayList<Integer> searched = indexFile.search("2345 Test St, Testville, Testland, 12345", "=");
            assertEquals(1, searched.size());
            assertEquals(9, searched.get(0));
            ArrayList<Integer> searched2 = indexFile.search("Terminology St 176, Summerholm, Guadeloupe, 673843", "=");
            assertEquals(0, searched2.size());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void removeItemFromCell() {
    }

    @Test
    public void addItemToCell() {
    }

    @Test
    public void findValue() {
    }

    @Test
    public void search() {
        initializeIndex();
        try {
            ArrayList<Integer> searched = indexFile.search("Terminology St 176, Summerholm, Guadeloupe, 673843", "=");
            assertEquals(1, searched.size());
            assertEquals(9, searched.get(0));
            ArrayList<Integer> searched2 = indexFile.search("Terminology St 176, Summerholm, Guadeloupe, 673843", ">");
            assertEquals(1, searched2.size());
            assertEquals(5, searched2.get(0));
            ArrayList<Integer> searched3 = indexFile.search("Terminology St 176, Summerholm, Guadeloupe, 673843", "<");
            assertEquals(8, searched3.size());
            ArrayList<Integer> searched4 = indexFile.search("Terminology St 176, Summerholm, Guadeloupe, 673843", ">=");
            assertEquals(2, searched4.size());
            ArrayList<Integer> searched5 = indexFile.search("Terminology St 176, Summerholm, Guadeloupe, 673843", "<=");
            assertEquals(9, searched5.size());
            ArrayList<Integer> searched6 = indexFile.search("Terminology St 176, Summerholm, Guadeloupe, 673843", "<>");
            assertEquals(9, searched6.size());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}