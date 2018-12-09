package protocol;

import ecs.Metadata;

public class Message implements IMessage {
	Status status;
	K key;
	V value;
	Metadata metadata;
	/**
	 * flag indicating if this message is of move data process when adding/removing servers
	 */
	boolean isBatchData = false;

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
	public Metadata getMetadata() {
		return metadata;
	}

	@Override
	public boolean isBatchData() {
		return isBatchData;
	}

	@Override
	public void setBatchData() {
		this.isBatchData = true;
	}

	@Override
	public String toString() {
		String keyString = key == null ? "metadata" : getKey();
		return status.name() + "<" + keyString + '>';
	}
}
