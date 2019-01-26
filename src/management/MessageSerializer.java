package management;

import ecs.ExternalConfigurationService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;

public class MessageSerializer {
    private static Logger LOG = LogManager.getLogger(ExternalConfigurationService.ECS_LOG);

    /**
     * converts a {@link T}-typed message to byte array and sends it over the network
     *
     * @param message the {@link T}-typed message to be converted
     * @return byte array of the {@link T}-type message
     */
    public static <T> byte[] serialize(T message) {
        if (message == null) {
            return null;
        }

        try (ByteArrayOutputStream byteArrayOS = new ByteArrayOutputStream();
             ObjectOutputStream output = new ObjectOutputStream(byteArrayOS)) {

            output.writeObject(message);
            return byteArrayOS.toByteArray();
        } catch (IOException e) {
            LOG.error(e);
        }
        return null;
    }

    /**
     * converts a byte array received from the network to a {@link ConfigMessage}
     *
     * @param msgBytes the byte array to be converted
     * @return the {@link ConfigMessage} corresponding to the byte array msgBytes
     */
    @SuppressWarnings("unchecked")
    public static <T> T deserialize(byte[] msgBytes) {
        if (msgBytes == null) {
            return null;
        }
        try (ByteArrayInputStream byteArrayIS = new ByteArrayInputStream(msgBytes);
             ObjectInputStream input = new ObjectInputStream(byteArrayIS)) {
            try {
                return (T) input.readObject();
            } catch (ClassNotFoundException e) {
                LOG.error("Error when casting Object to Message: " + e);
            }
        } catch (IOException e) {
            LOG.error(e);
        }
        return null;
    }

//    /**
//     * converts a {@link FailureReportMessage} to byte array and sends it over the network
//     *
//     * @param message the {@link FailureReportMessage} to be converted
//     * @return byte array of the {@link FailureReportMessage}
//     */
//    public static byte[] marshall(FailureReportMessage message) throws IOException {
//        if (message == null) {
//            return null;
//        }
//
//        try (ByteArrayOutputStream byteArrayOS = new ByteArrayOutputStream();
//             ObjectOutputStream output = new ObjectOutputStream(byteArrayOS)) {
//
//            output.writeObject(message);
//            return byteArrayOS.toByteArray();
//        }
//    }
//
//    /**
//     * converts a byte array received from the network to a {@link FailureReportMessage}
//     *
//     * @param msgBytes the byte array to be converted
//     * @return the {@link FailureReportMessage} corresponding to the byte array msgBytes
//     */
//    public static FailureReportMessage unmarshallFailureReportMessage(byte[] msgBytes) throws IOException{
//        if (msgBytes == null) {
//            return null;
//        }
//        try (ByteArrayInputStream byteArrayIS = new ByteArrayInputStream(msgBytes);
//             ObjectInputStream input = new ObjectInputStream(byteArrayIS)) {
//            try {
//                return (FailureReportMessage) input.readObject();
//            } catch (ClassNotFoundException e) {
//                LOG.error("Error when casting Object to FailureReportMessage: " + e);
//            }
//        }
//        return null;
//    }

}
