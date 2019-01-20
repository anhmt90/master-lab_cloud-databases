package protocol.kv;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import ecs.NodeInfo;
import ecs.Metadata;
import protocol.kv.IMessage.Status;
import server.app.Server;
import util.HashUtils;
import util.StringUtils;

public class MessageMarshaller {

    private static final int NODE_INFO_SIZE = 4 + 2 + 16 + 16;
    private static Logger LOG = LogManager.getLogger(Server.SERVER_LOG);

    /**
     * converts a {@link IMessage}  to an byte array to send over the network
     *
     * @param message
     * @return
     */
    public static byte[] marshall(IMessage message) {
        if (message == null) {
            return null;
        }
        Status status = message.getStatus();
        if (status.equals(Status.SERVER_NOT_RESPONSIBLE)) {
            return marshallMetadata(message);
        } else if (status.equals(Status.SERVER_STOPPED) || status.equals(Status.SERVER_WRITE_LOCK) || status.equals(Status.REQUEST_METADATA)) {
            return new byte[]{status.getCode()};
        }
        byte[] keyBytes = message.getK() != null ? message.getK().get() : new byte[]{};
        byte[] valueBytes = message.getV() != null ? message.getV().get() : new byte[]{};
        byte[] output = new byte[1 + 1 + 3 + keyBytes.length + valueBytes.length];

        final short BUFFER_CAPACITY = 4;
        final short VAL_LENGTH_NUM_BYTES = 3;
        ByteBuffer valueLengthBuffer = ByteBuffer.allocate(BUFFER_CAPACITY).putInt(valueBytes.length);

        output[0] = status.getCode();
        output[1] = (byte) (message.isBatchData() ? 1 : 0);

        System.arraycopy(valueLengthBuffer.array(), 1, output, 1 + 1, VAL_LENGTH_NUM_BYTES);
        System.arraycopy(keyBytes, 0, output, 1 + 1 + VAL_LENGTH_NUM_BYTES, keyBytes.length);
        System.arraycopy(valueBytes, 0, output, 1 + 1 + VAL_LENGTH_NUM_BYTES + keyBytes.length, valueBytes.length);
        return output;
    }

    /**
     * converts a byte array received from the network to an {@link IMessage}
     *
     * @param msgBytes
     * @return
     */
    public static IMessage unmarshall(byte[] msgBytes) {
        if (msgBytes == null || msgBytes[0] == 0x00) {
            LOG.error("UNMARSHALLING NULL 1 ### " + msgBytes.length);
            return null;
        }
        Status status = Status.getByCode(msgBytes[0]);
        if (status == null) {
            LOG.error("UNMARSHALLING NULL 2 ### " + msgBytes.length);
            return null;
        }

        if (status.equals(Status.SERVER_NOT_RESPONSIBLE)) {
            return unmarshallMetadata(msgBytes, status);
        }
        if (msgBytes.length < 16)
            return new Message(status);

        boolean isBatchData = msgBytes[1] == 1 ? true : false;

        int valLength = getValueLength(msgBytes[2], msgBytes[3], msgBytes[4]);
        byte[] keyBytes = new byte[16];

        System.arraycopy(msgBytes, 1 + 1 + 3, keyBytes, 0, keyBytes.length);

        byte[] valBytes = new byte[valLength];
        System.arraycopy(msgBytes, 1 + 1 + 3 + keyBytes.length, valBytes, 0, valLength);

        IMessage message = (valLength == 0) ?
                new Message(status, new K(keyBytes))
                : new Message(status, new K(keyBytes), new V(valBytes));

        if (isBatchData)
            message.setBatchData();
        return message;
    }

    /**
     * Unmarshalls a byte array that contains a metadata object
     *
     * @param msgBytes the byte array containing all information for the unmarshalling
     * @param status   the status of the message to be unmarshalled
     * @return Message containing the metadata from the byte array
     */
    private static IMessage unmarshallMetadata(byte[] msgBytes, Status status) {
        int metadataLength = msgBytes[1]; // currenty support upto 256 nodes
        Metadata metadata = new Metadata();
        for (int i = 0; i < metadataLength; i++) {
            byte[] hostBytes = {msgBytes[2 + i * 38], msgBytes[3 + i * 38], msgBytes[4 + i * 38], msgBytes[5 + i * 38]};
            String host = "";
            try {
                InetAddress hostAddress = InetAddress.getByAddress(hostBytes);
                host = hostAddress.getHostAddress();
            } catch (UnknownHostException ex) {
                return null;
            }

            byte[] portBytes = {msgBytes[6 + i * 38], msgBytes[7 + i * 38]};
            int port = (portBytes[0] << 8) & 0x0000ff00 |
                    (portBytes[1] << 0) & 0x000000ff;
            byte[] hashRangeStartBytes = new byte[16];
            byte[] hashRangeEndBytes = new byte[16];
            System.arraycopy(msgBytes, 8 + i * 38, hashRangeStartBytes, 0, hashRangeStartBytes.length);
            System.arraycopy(msgBytes, 24 + i * 38, hashRangeEndBytes, 0, hashRangeEndBytes.length);
            String hashRangeStart = HashUtils.getHashStringOf(hashRangeStartBytes);
            String hashRangeEnd = HashUtils.getHashStringOf(hashRangeEndBytes);
            metadata.add(StringUtils.EMPTY_STRING, host, port, hashRangeStart, hashRangeEnd);
        }
        return new Message(status, metadata);
    }


    /**
     * Marshalls messages containing metadata so they can be sent over a socket
     *
     * @param message the message to be sent
     * @return a byte array containing all information from the message
     */
    public static byte[] marshallMetadata(IMessage message) {
        Metadata metadata = message.getMetadata();
        byte[] output = new byte[1 + 1 + metadata.getLength() * NODE_INFO_SIZE];


        output[0] = message.getStatus().getCode();
        output[1] = (byte) metadata.getLength();

        for (int j = 0; j < metadata.getLength(); j++) {
            NodeInfo meta = metadata.get(j);
            byte[] hostBytes = new byte[4];
            try {
                InetAddress host = InetAddress.getByName(meta.getHost());
                byte[] addressBytes = host.getAddress();
                if (addressBytes.length == 4) {
                    hostBytes = addressBytes;
                }
            } catch (UnknownHostException ex) {
                return null;
            }
            final short BUFFER_CAPACITY = 4;
            final short PORT_NUM_BYTES = 2;
            ByteBuffer portBuffer = ByteBuffer.allocate(BUFFER_CAPACITY).putInt(meta.getPort());
            byte[] portBytes = new byte[2];
            portBytes[0] = portBuffer.array()[2];
            portBytes[1] = portBuffer.array()[3];
            byte[] startBytes = meta.getWriteRange().getStartBytes();
            byte[] endBytes = meta.getWriteRange().getEndBytes();
            byte[] nodeMetadataBytes = new byte[hostBytes.length + portBytes.length + startBytes.length + endBytes.length];
            System.arraycopy(hostBytes, 0, nodeMetadataBytes, 0, hostBytes.length);
            System.arraycopy(portBytes, 0, nodeMetadataBytes, hostBytes.length, portBytes.length);
            System.arraycopy(startBytes, 0, nodeMetadataBytes, hostBytes.length + portBytes.length, startBytes.length);
            System.arraycopy(endBytes, 0, nodeMetadataBytes, nodeMetadataBytes.length - endBytes.length, endBytes.length);
            System.arraycopy(nodeMetadataBytes, 0, output, 2 + (j * 38), nodeMetadataBytes.length);
        }
        return output;
    }

    private static int getValueLength(byte first, byte second, byte third) {
        return ByteBuffer.wrap(new byte[]{0, first, second, third}).getInt();
    }

    public static boolean isMessageComplete(byte[] msgBytes, int bytesRead) {
        Status status = Status.getByCode(msgBytes[0]);
        if (status.equals(Status.SERVER_NOT_RESPONSIBLE)) {
            int metadataLength = msgBytes[1];
            return 1 + 1 + metadataLength * NODE_INFO_SIZE == bytesRead;
        } else if (status.equals(Status.SERVER_STOPPED) || status.equals(Status.SERVER_WRITE_LOCK)) {
            return 1 == bytesRead;
        }
        int valLength = getValueLength(msgBytes[2], msgBytes[3], msgBytes[4]);
        return 1 + 1 + 3 + 16 + valLength == bytesRead;
    }
}


