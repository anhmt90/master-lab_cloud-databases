package management;

import ecs.ExternalConfigurationService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocol.IMessage;
import protocol.K;
import protocol.Message;
import protocol.V;
import server.app.Server;

import java.io.*;
import java.nio.ByteBuffer;

public class ConfigMessageMarshaller {
    private static Logger LOG = LogManager.getLogger(ExternalConfigurationService.ECS_LOG);
    /**
     * converts a {@link ConfigMessage} to byte array and sends it over the network
     *
     * @param message the {@link ConfigMessage} to be converted
     * @return byte array of the {@link ConfigMessage}
     */
    public static byte[] marshall(ConfigMessage message) throws IOException {
        if (message == null) {
            return null;
        }

        try (ByteArrayOutputStream byteArrayOS = new ByteArrayOutputStream();
             ObjectOutputStream output = new ObjectOutputStream(byteArrayOS)) {

            output.writeObject(message);
            return byteArrayOS.toByteArray();
        }
    }

    /**
     * converts a byte array received from the network to a {@link ConfigMessage}
     *
     * @param msgBytes the byte array to be converted
     * @return the {@link ConfigMessage} corresponding to the byte array msgBytes
     */
    public static ConfigMessage unmarshall(byte[] msgBytes) throws IOException{
        if (msgBytes == null) {
            return null;
        }
        try (ByteArrayInputStream byteArrayIS = new ByteArrayInputStream(msgBytes);
             ObjectInputStream input = new ObjectInputStream(byteArrayIS)) {
            try {
                return (ConfigMessage) input.readObject();
            } catch (ClassNotFoundException e) {
                LOG.error("Error when casting Object to ConfigMessage: " + e);
            }
        }
        return null;
    }

    /**
     * converts a {@link FailureReportMessage} to byte array and sends it over the network
     *
     * @param message the {@link FailureReportMessage} to be converted
     * @return byte array of the {@link FailureReportMessage}
     */
    public static byte[] marshall(FailureReportMessage message) throws IOException {
        if (message == null) {
            return null;
        }

        try (ByteArrayOutputStream byteArrayOS = new ByteArrayOutputStream();
             ObjectOutputStream output = new ObjectOutputStream(byteArrayOS)) {

            output.writeObject(message);
            return byteArrayOS.toByteArray();
        }
    }

    /**
     * converts a byte array received from the network to a {@link FailureReportMessage}
     *
     * @param msgBytes the byte array to be converted
     * @return the {@link FailureReportMessage} corresponding to the byte array msgBytes
     */
    public static FailureReportMessage unmarshallFailureReportMessage(byte[] msgBytes) throws IOException{
        if (msgBytes == null) {
            return null;
        }
        try (ByteArrayInputStream byteArrayIS = new ByteArrayInputStream(msgBytes);
             ObjectInputStream input = new ObjectInputStream(byteArrayIS)) {
            try {
                return (FailureReportMessage) input.readObject();
            } catch (ClassNotFoundException e) {
                LOG.error("Error when casting Object to FailureReportMessage: " + e);
            }
        }
        return null;
    }

}
