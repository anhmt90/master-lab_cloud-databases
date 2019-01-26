package testing;

import org.junit.Test;
import server.app.Server;
import util.Validate;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Random;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import static util.StringUtils.splitEvery;

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

    @Test
    public void testByteString(){
        String testString = "test bytes     \n";
        String[] byteStrings = Arrays.toString(testString.getBytes()).replaceAll("[^0-9 ]", "").trim().split(" +");
        StringBuilder sb = new StringBuilder();
        for (String aByte : byteStrings) {
            while (aByte.length() < 3)
                aByte = "0" + aByte;
            sb.append(aByte);
        }
        System.out.println(sb.toString());
        assertThat(decodeStringFrom(sb.toString()).equals(testString), is(true));
    }

    private String decodeStringFrom(String byteString) {
        Validate.isTrue(byteString.length() % 3 == 0, "Invalid byteString");
        String[] byteStrings = splitEvery(byteString, 3);
        byte[] bytes = new byte[byteStrings.length];

        for (byte i = 0; i < byteStrings.length; i++)
            bytes[i] = Byte.valueOf(byteStrings[i]);

        return new String(bytes);
    }
}
