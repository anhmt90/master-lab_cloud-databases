package testing;

import org.junit.Test;
import server.app.Server;
import util.FileUtils;
import util.StringUtils;
import util.Validate;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.*;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import static protocol.mapreduce.Utils.NODEID_KEYBYTES_SEP;
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
    public void testByteString() {
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

    @Test
    public void testSplitString() {
        String path = "/mnt/Dante/Workspace/uni-project/cloud-databases/cloud-db/db/node3/37/b5/1d/19/4a/75/13/e4/5b/56/f6/52/4f/2d/51/f2/iwc_165072061944941fbf972bde34c4396956725162.node2.098097114";
        String fileName = Paths.get(path).getFileName().toString();
        String[] components = fileName.split("\\" + NODEID_KEYBYTES_SEP);
        Validate.isTrue(components.length == 3, "Invalid MR file name format");

        String byteString = components[2];
        Validate.isTrue(byteString.length() % 3 == 0, "Invalid byteString");
        String[] byteStrings = byteString.split("(?<=\\G...)");
        byte[] bytes = new byte[byteStrings.length];

        for (byte i = 0; i < byteStrings.length; i++)
            bytes[i] = Byte.valueOf(byteStrings[i]);

        System.out.println(new String(bytes));

    }

    @Test
    public void testGetUsername() {
        System.out.println(System.getProperty("user.name"));
        System.out.println(FileUtils.USER_DIR);
        System.out.println(FileUtils.WORKING_DIR);
    }

    @Test
    public void testSubstring() {
        String searchTerm = "bar bar foo";
        String[] words = searchTerm.split("\\s+");
        System.out.println(Arrays.toString(words));

        List<String> res = new ArrayList<>();
        for (int i = 0; i < words.length; i++) {
            for (int j = i + 1; j <= words.length; j++) {
                String[] substrings = Arrays.copyOfRange(words, i, j);
                StringBuilder sb = new StringBuilder();
                for (String word : substrings) {
                    sb.append(word + " ");
                }
                sb.setLength(sb.length() - 1);
                res.add(sb.toString());
            }
        }

        System.out.println(Arrays.toString(res.toArray()));


        HashMap<String, String> output = new HashMap<>();
        HashMap<String, String> vals = new HashMap<>();
        vals.put("foo", "bar bar bar foo foo foo foee");
        vals.put("bar", "bar bar1 bar1 bar2 bar foo foo foo foe foe foo1");
        List<String> bestMatches = new ArrayList<>();
        int maxMatchingLength = 0;
        for (Map.Entry<String, String> entry : vals.entrySet()) {
            String key = entry.getKey();
            String val = entry.getValue();

            for (String input : res) {
                int inputWords = input.split("\\s+").length;
                if (val.contains(input)) {
                    if (inputWords >= maxMatchingLength) {
                        bestMatches.add(input);
                        maxMatchingLength = inputWords;
                        bestMatches.removeIf(bm -> bm.split("\\s+").length < inputWords);
                    }
                }
            }
            for (String match : bestMatches) {
                output.put(match, output.containsKey(match) ? output.get(match) + "\n" + key : key);
            }

            System.out.println(Arrays.toString(output.entrySet().toArray()));

        }
    }
}
