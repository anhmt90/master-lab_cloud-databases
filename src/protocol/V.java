package protocol;

import java.io.Serializable;
import java.util.Arrays;

public class V implements Serializable {
    private byte[] value;

    public V(byte[] value) {
        this.value = value;
    }

    public byte[] get() {
        return value;
    }

    public String getString() {
        return new String(value);
    }

    @Override
    public String toString() {
        return Arrays.toString(value);
    }

    @Override
    public boolean equals(Object o) {
        if(o instanceof V) {
            V other = (V) o;
            return Arrays.equals(value, other.get());
        }
        return false;
    }
}
