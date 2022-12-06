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
        var catalogDir = new File("data");
        if (catalogDir.exists()) {
            for (var file : catalogDir.listFiles()) {
                for (var file2 : file.listFiles()) {
                    file2.delete();
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
    }

    @Test
    public void testCreateTable(){

    }

    @Test
    public void testDeleteFromTable() throws IOException {
        Commands.parseUserCommand("CREATE TABLE test (col1 TEXT, col2 INT, col3 TEXT)");
        Commands.parseUserCommand("DROP TABLE test");
    }
}
