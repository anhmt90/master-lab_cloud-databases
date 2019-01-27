package protocol.kv;

import util.HashUtils;
import util.StringUtils;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class K implements Serializable {
    private final String key;

    public K(String key) {
        this.key = key;
    }

    public String get() {
        return key;
    }

    public String getHashed() {
        return HashUtils.hash(key);
    }

    public byte[] getBytes() {
        return key.getBytes(StandardCharsets.US_ASCII);
    }

    public String getByteString(){
        return StringUtils.encode(key);
    }

    @Override
    public String toString() {
        return key;
    }

    @Override
    public boolean equals(Object o) {
        if(o instanceof K) {
            K other = (K) o;
            return key.equals(other.get());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }
}
