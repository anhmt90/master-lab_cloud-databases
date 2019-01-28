package protocol.kv;

import ecs.Metadata;
import mapreduce.client.MRKeyComparator;
import util.StringUtils;

public class Message implements IMessage {
    private Status status;
    private K key;
    private V value;
    private Metadata metadata;

    private String MRToken;
    /**
     * flag indicating if this message is of move data process when adding/removing servers
     */
    boolean isInternal = false;

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

    public Message(Status status, Metadata metadata) {
        this.status = status;
        this.metadata = metadata;
    }

    @Override
    public String getKeyHashed() {
        return key.getHashed();
    }

    @Override
    public String getValue() {
        return value.get();
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
    public Metadata getMetadata() {
        return metadata;
    }

    @Override
    public boolean isInternal() {
        return isInternal;
    }

    @Override
    public String getMRToken() {
        return MRToken;
    }

    @Override
    public void setMRToken(String mrjobId) {
        this.MRToken = mrjobId;
        setInternal();
    }

    @Override
    public void setInternal() {
        this.isInternal = true;
    }

    @Override
    public String toString() {
        String keyString = key == null ? "metadata" : key.get();
        String toPrint = status.name() + "<" + keyString + '>';
        if (hasMRToken()) {
            toPrint += "\nToken<" + MRToken + ">";
            toPrint += "\nisInternal: " + isInternal;
            if (value != null)
                toPrint += "\nVAL<" + value.get() + ">";

        }
        return toPrint;
    }

    public static IMessage createPUTMessage(String key, String value) {
        if (value == null)
            return new Message(Status.PUT, new K(key));
        return new Message(Status.PUT, new K(key), new V(value));
    }

    @Override
    public boolean hasMRToken(){
        return !StringUtils.isEmpty(MRToken);
    }
}
