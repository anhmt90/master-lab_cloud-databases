package protocol;

import util.Validate;

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

    public static Message build(byte[] messageBytes) {
        Status status = Status.getByCode(messageBytes[0]);
        Validate.notNull(status, "Invalid message status. Message does not comply to the protocol.");
        byte keyLength = messageBytes[1];
        byte[] key = new byte[keyLength];
        System.arraycopy(messageBytes,2, key, 0, keyLength);
        byte[] value = new byte[messageBytes.length - 2 - keyLength];
        System.arraycopy(messageBytes, 2 + keyLength, value, 0, messageBytes.length);
        return new Message(status, new K(key), new V(value));
     }

}
