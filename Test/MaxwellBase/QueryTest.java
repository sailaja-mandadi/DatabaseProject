package MaxwellBase;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;


public class QueryTest {
    @Before
    public void setUp() throws IOException {
        var catDir = Settings.getCatalogDirectory();
        var userDir = Settings.getUserDataDirectory();
        // See if catalog directory exists and delete it if it does
//        var catalogDir = new File(catDir);
//        if (catalogDir.exists()) {
//            if (!catalogDir.delete()) {
//                throw new IOException("Could not delete catalog directory");
//            }
//        }
        // See if user data directory exists and delete it if it does
//        var userDataDir = new File(userDir);
//        if (userDataDir.exists()) {
//            if (!userDataDir.delete()) {
//                throw new IOException("Could not delete user data directory");
//            }
//        }
        Initialization.initializeCatalogDirectory();
    }

    @Test
    public void testInitialize() throws IOException {
        Initialization.initialize(new File("data"));
    }

    @Test
    public void testCreateTable(){

    }

    @Test
    public void testDeleteFromTable() throws IOException {
        Commands.parseUserCommand("CREATE TABLE test (col1 TEXT, col2 INT, col3 TEXT)");
        Commands.parseUserCommand("INSERT INTO students VALUES (\"John\", 20, 3)");
        Commands.parseUserCommand("INSERT INTO students VALUES (\"Jane\", 21, 4)");
        Commands.parseUserCommand("DELETE FROM students WHERE gpa = 3");
    }
}
