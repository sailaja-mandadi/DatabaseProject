package MaxwellBase;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class QueryTest {
    @Before
    public void setUp() throws IOException {
        var catalogDir = new File("data");
        if (catalogDir.exists()) {
            for (var file : Objects.requireNonNull(catalogDir.listFiles())) {
                for (var file2 : Objects.requireNonNull(file.listFiles())) {
                    if (!file2.delete()) {
                        System.out.println("Why not?");
                    }
                }
                if (!file.delete()) {
                    System.out.println("Why not?");
                }
            }
            if(!catalogDir.delete()){
                System.out.println("Why not?");
            }
        }
        Initialization.initialize(new File("data"));
    }

    @Test
    public void testInitialize() throws IOException {
        Initialization.initialize(new File("data"));
        assertNotNull(Table.tableTable);
        assertNotNull(Table.columnTable);
        assert Table.tableExists(Settings.maxwellBaseTables);
        assert Table.tableExists(Settings.maxwellBaseColumns);
    }

    @Test
    public void testCreateTable() throws IOException {
        Commands.parseUserCommand("CREATE TABLE test (col1 TEXT, col2 INT, col3 TEXT)");
        assert Table.tableExists("test");
    }

    @Test
    public void dropTable() throws IOException {
        testCreateTable();
        Commands.parseUserCommand("DROP TABLE test");
        assert !Table.tableExists("test");
    }


    @Test
    public void testInsert() throws IOException {
        testCreateTable();
        Table table = new Table("test", true);
        Commands.parseUserCommand("INSERT INTO test VALUES (test1, 1, test2)");
        Commands.parseUserCommand("INSERT INTO test VALUES (test3, 2, test4)");
        Commands.parseUserCommand("INSERT INTO test VALUES (test5, 1, test6)");
        assertEquals(3, table.search(null, null, null).size());
    }

    @Test
    public void testPrimaryKey() throws IOException {
        Commands.parseUserCommand("CREATE TABLE test (col1 TEXT PRIMARY_KEY, col2 INT, col3 TEXT)");
        assert Table.tableExists("test");
        Table test = new Table("test", true);
        assert test.indexExists("col1");
        assert !test.indexExists("col2");
        assert !test.indexExists("col3");
        Commands.parseUserCommand("INSERT INTO test VALUES (\"test1\", 1, \"test2\")");
        Commands.parseUserCommand("INSERT INTO test VALUES (\"test3\", 2, \"test4\")");
        Commands.parseUserCommand("INSERT INTO test VALUES (\"test5\", 1, \"test6\")");
        assertEquals(3, test.search(null, null, null).size(), "Should have 3 records");
        Commands.parseUserCommand("INSERT INTO test VALUES (\"test1\", 1, \"test2\")");
        assertEquals(3, test.search(null, null, null).size(), "Duplicate primary key detected");
    }

    @Test
    public void uniqueTest() throws IOException {
        Commands.parseUserCommand("CREATE TABLE test (col1 TEXT, col2 INT UNIQUE, col3 TEXT)");
        assert Table.tableExists("test");
        Table test = new Table("test", true);
        Commands.parseUserCommand("INSERT INTO test VALUES (\"test1\", 1, \"test2\")");
        Commands.parseUserCommand("INSERT INTO test VALUES (\"test3\", 2, \"test4\")");
        assertEquals(2, test.search(null, null, null).size(), "Should have 3 records");
        Commands.parseUserCommand("INSERT INTO test VALUES (\"test8\", 1, \"test\")");
        assertEquals(2, test.search(null, null, null).size(), "Duplicate unique key detected");
    }

    @Test
    public void testDeleteFromTable() throws IOException {
        testInsert();
        Table table = new Table("test", true);
        Commands.parseUserCommand("DELETE FROM test WHERE col2 = 1");
        assertEquals(1, table.search(null, null, null).size());
    }

    @Test
    public void testIndexSearch() throws IOException {
        testInsert();
        Commands.parseUserCommand("CREATE INDEX test (col2)");
        Table table = new Table("test", true);
        assert table.indexExists("col2");
        Commands.parseUserCommand("SELECT * FROM test WHERE col2 = 1");
    }

    @Test
    public void updateTest() throws IOException {
        testInsert();
        Table table = new Table("test", true);
        assertEquals(0, table.search("col2", 3, "=").size());
        Commands.parseUserCommand("UPDATE test SET col2 = 3 WHERE col1 = test5");
        assertEquals(1, table.search("col2", 3, "=").size());
    }

}
