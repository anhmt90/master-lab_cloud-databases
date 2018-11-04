package protocol;

import util.Validate;

import java.io.Serializable;

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
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public Status getStatus() {
        return status;
    }

    @Override
    public String toString() {
        return status.name() +"<" + key + ", " + value + '>';
    }
}
