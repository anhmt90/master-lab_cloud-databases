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
        this.value = value;
    }

    @Override
    public String getKey() {
        return key.get();
    }

    @Override
    public String getValue() {
        return value.get();
    }

    @Override
    public Status getStatus() {
        return status;
    }



}
