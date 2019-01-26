package protocol.kv;

import ecs.Metadata;

public class Message implements IMessage {
	private Status status;
	private K key;
	private V value;
	private Metadata metadata;

	private String mrjobId;
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
	public String getMrjobId() {
		return mrjobId;
	}

	@Override
	public void setMrjobId(String mrjobId) {
		this.mrjobId = mrjobId;
		setInternal();
	}

	@Override
	public void setInternal() {
		this.isInternal = true;
	}

	@Override
	public String toString() {
		String keyString = key == null ? "metadata" : getKeyHashed();
		return status.name() + "<" + keyString + '>';
	}

	public static IMessage createPUTMessage(String key, String value) {
		if (value == null)
			return new Message(Status.PUT, new K(key));
		return new Message(Status.PUT, new K(key), new V(value));
	}
}
