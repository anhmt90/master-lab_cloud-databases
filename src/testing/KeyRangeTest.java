package testing;

import ecs.KeyHashRange;
import junit.framework.TestCase;
import org.junit.Test;
import util.FileUtils;
import util.HashUtils;
import util.StringUtils;

import java.util.Arrays;

import static util.FileUtils.SEP;

public class KeyRangeTest extends TestCase {

    @Test
    public void testKeyRangeCharacteristics() {
        String node1Hash = new String(new char[8]).replace("\0", "3333");
        String node2Hash = new String(new char[8]).replace("\0", "9999");
        String node3Hash = new String(new char[8]).replace("\0", "eeee");

        KeyHashRange node1_range = new KeyHashRange(HashUtils.increaseHashBy1(node3Hash), node1Hash); // EEEE..EEEF - 3333..3333

        assertTrue(node1_range.inRange(node1Hash));
        assertTrue(node1_range.isWrappedAround());

        KeyHashRange newRange = new KeyHashRange(new String(new char[8]).replace("\0", "efef"),
                new String(new char[8]).replace("\0", "1010"));

        assertTrue(newRange.isWrappedAround() && newRange.isSubRangeOf(node1_range));
    }

    @Test
    public void testHashing() {
        String key = "key1";
        byte[] arrayA = HashUtils.getHashBytesOf(HashUtils.getHash(key));
        byte[] arrayB = HashUtils.getHashBytes(key);
        assertTrue("Not equal", Arrays.equals(arrayA, arrayB));
    }

}
