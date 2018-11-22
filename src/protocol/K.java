package protocol;

import util.HashUtils;

import java.io.Serializable;
import java.util.Arrays;

public class K implements Serializable {
    private final byte[] key;

    public K(byte[] key) {
        this.key = key;
    }

    public byte[] get() {
        return key;
    }

    public String getString() {
        return HashUtils.getHashStringOf(key);
    }

    @Override
    public String toString() {
        return Arrays.toString(key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        K k = (K) o;
        return Arrays.equals(key, k.key);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(key);
    }
}
