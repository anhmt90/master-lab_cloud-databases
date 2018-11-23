package protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import ecs.NodeInfo;
import ecs.Metadata;
import protocol.IMessage.Status;
import util.HashUtils;

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
        if(message.getStatus().equals(Status.SERVER_NOT_RESPONSIBLE)) {
        	return marshallNotResponsible(message);
        }
        byte[] keyBytes = message.getK() != null ? message.getK().get() : new byte[]{};
        byte[] valueBytes = message.getV() != null ? message.getV().get() : new byte[]{};
        byte[] output = new byte[1 + 3 + keyBytes.length + valueBytes.length];

        final short BUFFER_CAPACITY = 4;
        final short VAL_LENGTH_NUM_BYTES = 3;
        ByteBuffer valueLengthBuffer = ByteBuffer.allocate(BUFFER_CAPACITY).putInt(valueBytes.length);

        output[0] = message.getStatus().getCode();

        System.arraycopy(valueLengthBuffer.array(), 1, output, 1, VAL_LENGTH_NUM_BYTES);
        System.arraycopy(keyBytes, 0, output, 1 + VAL_LENGTH_NUM_BYTES, keyBytes.length);
        System.arraycopy(valueBytes, 0, output, 1 + VAL_LENGTH_NUM_BYTES + keyBytes.length, valueBytes.length);
        return output;
    }

    /**
     * converts a byte array received from the network to an {@link IMessage}
     * @param msgBytes
     * @return
     */
    public static IMessage unmarshall(byte[] msgBytes) {
        if (msgBytes == null || msgBytes[0] == 0x00) {
            return null;
        }
        Status status = Status.getByCode(msgBytes[0]);
        if (status == null)
            return null;
        
        if (status.equals(Status.SERVER_NOT_RESPONSIBLE)) {
        	int metadataSize = msgBytes[1];
        	Metadata metadata = new Metadata();
        	for (int i = 0; i < metadataSize; i++) {
        		String host = "";
        		for(int j = 0; j < 4; j++) {
        			host = host + Byte.toString(msgBytes[2 + j + i*38]);
        		}
        		byte[] portBytes = {msgBytes[6 + i*38], msgBytes[7 + i*38]};
        		int port = (portBytes[0]<< 8)&0x0000ff00|
        			       (portBytes[1]<< 0)&0x000000ff;;
        		String hashRangeStart = "";
        		String hashRangeEnd = "";
        		for(int j = 0; j < 16; j++) {
        			hashRangeStart = hashRangeStart + Byte.toString(msgBytes[7 + j + i*38]);
        			hashRangeEnd = hashRangeEnd + Byte.toString(msgBytes[24 + j + i*38]);
        		}
        		metadata.add(host, port, hashRangeStart, hashRangeEnd);
        	}
        	return new Message(status, metadata);
        }

        int valLength = ByteBuffer.wrap(new byte[]{0, msgBytes[1], msgBytes[2], msgBytes[3]}).getInt();

        byte[] keyBytes = new byte[16];
        System.arraycopy(msgBytes, 1 + 3, keyBytes, 0, keyBytes.length);

        byte[] valBytes = new byte[valLength];
        System.arraycopy(msgBytes, 1 + 3 + keyBytes.length, valBytes, 0, valLength);
        if (valLength == 0)
            return new Message(status, new K(keyBytes));
        return new Message(status, new K(keyBytes), new V(valBytes));
    }

    
    public static byte[] marshallNotResponsible(IMessage message) {
    	Metadata metadata = message.getMetadata();
    	byte[] output = new byte[1 + 1 + metadata.getSize()*(4 + 2 + 16 + 16)];
    	
        
    	output[0] = message.getStatus().getCode();    	
    	output[1] = (byte)metadata.getSize();
    	
    	for(int j = 0; j < metadata.getSize(); j++) {
    		NodeInfo meta = metadata.get().get(j);
    		String[] host = meta.getHost().split(".");
    		byte[] hostBytes = new byte[4];
    		for(int i = 0; i<host.length; i++) {
    			hostBytes[i] = Byte.parseByte(host[i]);
    		}
            final short BUFFER_CAPACITY = 4;
            final short PORT_NUM_BYTES = 2;
            ByteBuffer portBuffer = ByteBuffer.allocate(BUFFER_CAPACITY).putInt(meta.getPort());
            byte[] portBytes = new byte[2];
            portBytes[0] = portBuffer.array()[2];
            portBytes[1] = portBuffer.array()[3];
            byte[] startBytes = meta.getRange().getStartBytes();
            byte[] endBytes = meta.getRange().getEndBytes();
            byte[] nodeMetadataBytes = new byte[hostBytes.length + portBytes.length + startBytes.length + endBytes.length];
            System.arraycopy(hostBytes, 0, nodeMetadataBytes, 0, hostBytes.length);
            System.arraycopy(portBytes, 0, nodeMetadataBytes, hostBytes.length, portBytes.length);
            System.arraycopy(startBytes, 0, nodeMetadataBytes, hostBytes.length + portBytes.length, startBytes.length);
            System.arraycopy(endBytes, 0, nodeMetadataBytes, nodeMetadataBytes.length - endBytes.length, endBytes.length);
            System.arraycopy(nodeMetadataBytes, 0, output, 2 + (j*38), nodeMetadataBytes.length);
    	}
    	return output;
    }
    
}
