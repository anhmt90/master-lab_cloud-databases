package protocol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

import protocol.IMessage;

public class MessageMarshaller {

	public static byte[] marshall(IMessage message) {
		if (message == null) {
			return null;
		}
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out = null;
		try {
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeObject(message);
			oos.flush();
		} catch (IOException ex) {
			return null;
		}
		return bos.toByteArray();
	}

	public static IMessage unmarshall(byte[] marshalledMessage) {
		if (marshalledMessage == null) {
			return null;
		}
		ByteArrayInputStream bis = new ByteArrayInputStream(marshalledMessage);
		ObjectInput in = null;
		try {
			in = new ObjectInputStream(bis);
			IMessage message = (IMessage) in.readObject();
			return message;
		} catch (IOException ex) {
			return null;
		} catch (ClassNotFoundException e) {
			return null;
		}
	}

}
