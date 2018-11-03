package protocol;

public class K {
    byte[] key;

    public K(byte[] key) {
        this.key = key;
    }

    public byte[] get() {
        return key;
    }
}
