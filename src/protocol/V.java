package protocol;

import java.io.Serializable;
import java.util.Arrays;

public class V implements Serializable {
    byte[] value;

    public V(byte[] value) {
        this.value = value;
    }

    public byte[] get() {
        return value;
    }

    @Override
    public String toString() {
        return Arrays.toString(value);
    }
}
