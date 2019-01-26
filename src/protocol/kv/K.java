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
        return Arrays.toString(getBytes()).replaceAll("[^a-z ]", "").trim();
    }

    @Override
    public String toString() {
        return key;
    }

    @Override
    public boolean equals(Object o) {
        if (this.equals(o)) return true;
        if (o == null || getClass() != o.getClass()) return false;
        K k = (K) o;
        return key.equals(k.get());
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }
}
