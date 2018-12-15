package testing;

import ecs.KeyHashRange;
import junit.framework.TestCase;
import org.junit.Test;
import util.HashUtils;

import java.util.Arrays;

public class KeyRangeTest extends TestCase {

    @Test
    public void testKeyRangeCharacteristics() {
        String node1Hash = new String(new char[8]).replace("\0", "3333");
        String node2Hash = new String(new char[8]).replace("\0", "9999");
        String node3Hash = new String(new char[8]).replace("\0", "eeee");

        KeyHashRange node1_range = new KeyHashRange(HashUtils.increaseHashBy1(node3Hash), node1Hash); // EEEE..EEEF - 3333..3333

        assertTrue(node1_range.contains(node1Hash));
        assertTrue(node1_range.isWrappedAround());

        KeyHashRange newRange = new KeyHashRange(new String(new char[8]).replace("\0", "efef"),
                new String(new char[8]).replace("\0", "1010"));

        assertTrue(newRange.isWrappedAround() && newRange.isSubRangeOf(node1_range));
    }

    @Test
    public void testHashing() {
        String key = "key1";
        byte[] arrayA = HashUtils.getHashBytesOf(HashUtils.hash(key));
        byte[] arrayB = HashUtils.digest(key);
        assertTrue("Not equal", Arrays.equals(arrayA, arrayB));
    }

    @Test
    public void testEdgeRange() {
        String node1Hash = new String(new char[8]).replace("\0", "3333");
        String node2Hash = new String(new char[8]).replace("\0", "9999");
        String node3Hash = new String(new char[8]).replace("\0", "eeee");

        KeyHashRange node1_range = new KeyHashRange(HashUtils.increaseHashBy1(node3Hash), node1Hash); // EEEE..EEEF - 3333..3333
        String key1 = node3Hash;
        String key2 = node1Hash;
        String key3 = new String(new char[8]).replace("\0", "ffff");
        String key4 = new String(new char[8]).replace("\0", "0000");

        assertTrue(!node1_range.contains(key1));
        assertTrue(node1_range.contains(key2));
        assertTrue(node1_range.contains(key3));
        assertTrue(node1_range.contains(key4));
        assertTrue(!node1_range.contains(HashUtils.increaseHashBy(node1Hash, 2)));
        assertTrue(!node1_range.contains(HashUtils.increaseHashBy(node1Hash, 3)));

    }

}
