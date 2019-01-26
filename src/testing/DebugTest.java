package testing;

import org.junit.Test;
import server.app.Server;

import java.io.IOException;
import java.util.Arrays;

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

    @Test
    public void testString() {
        String input = "This is my file, yes my file My file.. ? ! , : ; / \\ |\" ^ * + = _( ) { } [ ] < >\n";
        String[] changedInput = input.toLowerCase().replaceAll("[^a-z ]", "").trim().split(" +");
        System.out.println(Arrays.toString(changedInput));
    }
}
