package protocol;

public class Message implements IMessage {
    Status status;
    K key;
    V value;

    public Message(Status status) {
        this.status = status;
    }

    public Message(Status status, K key) {
        this.status = status;
        this.key = key;
    }

    public Message(Status status, V value) {
        this.status = status;
        this.value = value;
    }

    public Message(Status status, K key, V value) {
        this.status = status;
        this.key = key;
        this.value = value;
    }

    public Message(K key, V value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public String getKey() {
        return key.getString();
    }


    @Override
    public String getValue() {
        return value.getString();
    }

    public K getK() {
        return key;
    }

    public V getV() {
        return value;
    }

    @Override
    public Status getStatus() {
        return status;
    }

    @Override
    public String toString() {
        return status.name() + "<" + key + ", " + value + '>';
    }
}
