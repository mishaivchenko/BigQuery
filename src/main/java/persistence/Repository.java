package persistence;

import java.io.IOException;

public interface Repository {

    void addTables(int count) throws IOException;
}
