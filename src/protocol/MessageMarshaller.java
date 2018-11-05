package protocol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

import protocol.IMessage;
import util.Validate;

public class MessageMarshaller {

    /**
     * converts a {@link IMessage}  to an byte array to send over the network
     * @param message
     * @return
     */
    public static byte[] marshall(IMessage message) {
        if (message == null) {
            return null;
        }
        byte[] keyBytes = message.getKey().get();
        byte[] valueBytes = message.getValue() != null ? message.getValue().get() : new byte[]{};
        byte[] output = new byte[1 + 1 + 3 + keyBytes.length + valueBytes.length];

        final short BUFFER_CAPACITY = 4;
        final short VAL_LENGTH_NUM_BYTES = 3;
        ByteBuffer valueLengthBuffer = ByteBuffer.allocate(BUFFER_CAPACITY).putInt(valueBytes.length);

        output[0] = message.getStatus().getCode();
        output[1] = (byte) keyBytes.length;

        System.arraycopy(valueLengthBuffer.array(), 1, output, 2, VAL_LENGTH_NUM_BYTES);
        System.arraycopy(keyBytes, 0, output, 2 + VAL_LENGTH_NUM_BYTES, keyBytes.length);
        System.arraycopy(valueBytes, 0, output, 2 + VAL_LENGTH_NUM_BYTES + keyBytes.length, valueBytes.length);
        return output;
    }

    /**
     * converts a byte array received from the network to an {@link IMessage}
     * @param msgBytes
     * @return
     */
    public static IMessage unmarshall(byte[] msgBytes) {
        if (msgBytes == null) {
            return null;
        }
        IMessage.Status status = IMessage.Status.getByCode(msgBytes[0]);
        if (status == null)
            return null;

        byte keyLength = msgBytes[1];
        int valLength = ByteBuffer.wrap(new byte[]{0, msgBytes[2], msgBytes[3], msgBytes[4]}).getInt();


        byte[] keyBytes = new byte[keyLength];
        System.arraycopy(msgBytes, 1 + 1 + 3, keyBytes, 0, keyLength);

        byte[] valBytes = new byte[valLength];
        System.arraycopy(msgBytes, 1 + 1 + 3 + keyLength, valBytes, 0, valLength);
        if (valLength == 0)
            return new Message(status, new K(keyBytes));
        return new Message(status, new K(keyBytes), new V(valBytes));
    }

}
