package protocol.kv;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class V implements Serializable {
    private String value;

    public V(String value) {
        this.value = value;
    }

    public String get() {
        return value;
    }

    public byte[] getBytes() {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if(o instanceof V) {
            V other = (V) o;
            return value.equals(other.get());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }
}
