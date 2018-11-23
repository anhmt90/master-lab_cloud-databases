package testing;

import junit.framework.TestCase;
import org.junit.Test;
import util.FileUtils;
import util.HashUtils;
import util.StringUtils;

import java.util.Arrays;

import static util.FileUtils.SEP;

public class KeyRangeTest extends TestCase {

    @Test
    public void test() {
        String start = "940890";
        String end = "b10010";
        while (start.length() < 32) {
            start = '0' + start;
            end = '0' + end;
        }

        String commonPrefix = StringUtils.getLongestCommonPrefix(start, end);
        String commonParentFolder = StringUtils.insertCharEvery(commonPrefix, SEP.charAt(0), 2);

    }

    @Test
    public void testHashing() {
        String key = "key1";
        byte[] arrayA = HashUtils.getHashBytesOf(HashUtils.getHash(key));
        byte[] arrayB = HashUtils.getHashBytes(key);
        assertTrue("Not equal", Arrays.equals(arrayA, arrayB));
    }


}
