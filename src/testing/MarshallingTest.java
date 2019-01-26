package testing;

import management.MessageSerializer;
import protocol.kv.*;
import protocol.kv.IMessage.Status;

import java.nio.charset.StandardCharsets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import ecs.Metadata;
import util.HashUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MarshallingTest {
    private static Logger LOG = LogManager.getLogger(AllTests.TEST_LOG);

    @Test
    public void testSerializeMetadata() {
        Metadata metadata = new Metadata();
        String name1 = "node1";
        String name2 = "node2";
        String name3 = "node3";
        String host1 = "127.9.234.1";
        String host2 = "123.94.75.2";
        String host3 = "127.0.0.1";
        int port1 = 46781;
        int port2 = 35678;
        int port3 = 54321;
        String hashKey1 = HashUtils.hash(String.format("%s:%d", host1, port1));
        String hashKey2 = HashUtils.hash(String.format("%s:%d", host2, port2));
        String hashKey3 = HashUtils.hash(String.format("%s:%d", host3, port3));
        metadata.add(name1, host1, port1, hashKey1, hashKey2);
        metadata.add(name2, host2, port2, hashKey2, hashKey3);
        metadata.add(name3, host3, port3, hashKey3, hashKey1);
        Message message = new Message(Status.SERVER_NOT_RESPONSIBLE, metadata);
        byte[] serializedMessage = MessageSerializer.serialize(message);
        IMessage deserializedMessage = MessageSerializer.deserialize(serializedMessage);
        assertEquals(Status.SERVER_NOT_RESPONSIBLE, deserializedMessage.getStatus());
        for (int i = 0; i < metadata.getLength(); i++) {
            assertEquals(message.getMetadata().get(i).getHost(), deserializedMessage.getMetadata().get(i).getHost());
            assertEquals(message.getMetadata().get(i).getPort(), deserializedMessage.getMetadata().get(i).getPort());
            assertEquals(message.getMetadata().get(i).getWriteRange().getStart(), deserializedMessage.getMetadata().get(i).getWriteRange().getStart());
            assertEquals(message.getMetadata().get(i).getWriteRange().getEnd(), deserializedMessage.getMetadata().get(i).getWriteRange().getEnd());
        }
    }

    @Test
    public void testNormalSerialization() {
        IMessage message = new Message(Status.GET, new K("thiskey"), new V("thisvalue"));
        byte[] serializedMessage = MessageSerializer.serialize(message);
        serializeDeserializeAssert(message);

    }

    @Test
    public void testNoKVSerialization() {
        IMessage message = new Message(Status.SERVER_STOPPED);
        byte[] serializedMessage = MessageSerializer.serialize(message);
        IMessage deserializedMessage = MessageSerializer.deserialize(serializedMessage);
        assertTrue(message.getStatus().equals(deserializedMessage.getStatus()));
    }

    @Test
    public void testNoValueSerialization() {
        IMessage message = new Message(Status.GET, new K("thiskey"));
        serializeDeserializeAssert(message);
    }

    private void serializeDeserializeAssert(IMessage message) {
        byte[] serializedMessage = MessageSerializer.serialize(message);
        IMessage deserializedMessage = MessageSerializer.deserialize(serializedMessage);
        assertTrue(message.getStatus().equals(deserializedMessage.getStatus()));
        assertEquals(message.getK().getHashed(), deserializedMessage.getK().getHashed());
        assertEquals(message.getV().get(), deserializedMessage.getV().get());

    }
}
