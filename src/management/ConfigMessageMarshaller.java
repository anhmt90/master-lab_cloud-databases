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
             BufferedOutputStream bufferedOS = new BufferedOutputStream(byteArrayOS);
             ObjectOutputStream output = new ObjectOutputStream(bufferedOS)) {

            output.writeObject(message);
            return byteArrayOS.toByteArray();
        }
    }

    /**
     * converts a byte array received fSerrom the network to a {@link ConfigMessage}
     *
     * @param msgBytes the byte array to be converted
     * @return the {@link ConfigMessage} corresponding to the byte array msgBytes
     */
    public static ConfigMessage unmarshall(byte[] msgBytes) throws IOException{
        if (msgBytes == null) {
            return null;
        }
        try (ByteArrayInputStream byteArrayIS = new ByteArrayInputStream(msgBytes);
             BufferedInputStream bufferedIS = new BufferedInputStream(byteArrayIS);
             ObjectInputStream input = new ObjectInputStream(bufferedIS)) {
            try {
                return (ConfigMessage) input.readObject();
            } catch (ClassNotFoundException e) {
                LOG.error("Error when casting Object to ConfigMessage: " + e);
            }
        }
        return null;
    }

}
