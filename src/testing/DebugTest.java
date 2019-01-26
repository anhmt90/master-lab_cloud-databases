package testing;

import org.junit.Test;
import server.app.Server;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Random;

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

    @Test
    public void testNanoSecond() {
        System.out.println(System.nanoTime());
    }

    @Test
    public void givenUsingPlainJava_whenGeneratingRandomStringUnbounded_thenCorrect() {
        byte[] array = new byte[7]; // length is bounded by 7
        new Random().nextBytes(array);
        String generatedString = new String(array, Charset.forName("UTF-8"));

        System.out.println(generatedString);
    }
}
