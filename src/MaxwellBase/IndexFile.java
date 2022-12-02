package MaxwellBase;

import java.io.FileNotFoundException;
import java.io.IOException;

public class IndexFile extends DatabaseFile{
    public IndexFile(String name) throws FileNotFoundException {
        super(name);
    }

    public int splitPage(int page) throws IOException {
        return 0;
    }
}
