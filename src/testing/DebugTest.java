package testing;

import org.junit.Test;
import server.app.Server;

import java.io.IOException;

public class DebugTest {

    @Test
    public void testServer() {
        String[] args = new String[]{"node0", "50000", "60000", "ERROR"};
        try {
            Server.main(args);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
