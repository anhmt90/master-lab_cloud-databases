package protocol;

import java.io.Serializable;
import java.util.Arrays;

public class K implements Serializable {
    byte[] key;

    public K(byte[] key) {
        this.key = key;
    }

    public byte[] get() {
        return key;
    }

    @Override
    public String toString() {
        return Arrays.toString(key);
    }
}
