package protocol;

import java.io.Serializable;
import java.util.Arrays;

public class K implements Serializable {
    private byte[] key;

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

    @Override
    public boolean equals(Object o) {
        if(o instanceof K) {
            K other = (K) o;
            return Arrays.equals(key, other.get());
        }
        return false;
    }
}
