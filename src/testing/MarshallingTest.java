package testing;

import protocol.*;
import protocol.IMessage.Status;
import junit.framework.TestCase;

import java.nio.charset.StandardCharsets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import ecs.Metadata;
import util.HashUtils;

public class MarshallingTest extends TestCase {
	private static Logger LOG = LogManager.getLogger(AllTests.TEST_LOG);

	@Test
    public void testMetadataMarshalling() {
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
		String hashKey1 = HashUtils.getHash(String.format("%s:%d", host1, port1));
		String hashKey2 = HashUtils.getHash(String.format("%s:%d", host2, port2));
		String hashKey3 = HashUtils.getHash(String.format("%s:%d", host3, port3));
		metadata.add(name1, host1, port1, HashUtils.increaseHashBy1(hashKey3), hashKey1);
		metadata.add(name2, host2, port2, HashUtils.increaseHashBy1(hashKey1), hashKey2);
		metadata.add(name3, host3, port3, HashUtils.increaseHashBy1(hashKey2), hashKey3);
        Message message = new Message(Status.SERVER_NOT_RESPONSIBLE, metadata);
        byte[] marshalledMessage = MessageMarshaller.marshall(message);
        IMessage unmarshalledMessage = MessageMarshaller.unmarshall(marshalledMessage);
        assertEquals(Status.SERVER_NOT_RESPONSIBLE, unmarshalledMessage.getStatus());
        for(int i = 0; i < metadata.getSize(); i++) {
        	assertEquals(message.getMetadata().get().get(i).getHost(), unmarshalledMessage.getMetadata().get().get(i).getHost());
        	assertEquals(message.getMetadata().get().get(i).getPort(), unmarshalledMessage.getMetadata().get().get(i).getPort());
        	assertEquals(message.getMetadata().get().get(i).getRange().getStart(), unmarshalledMessage.getMetadata().get().get(i).getRange().getStart());
        	assertEquals(message.getMetadata().get().get(i).getRange().getEnd(), unmarshalledMessage.getMetadata().get().get(i).getRange().getEnd());
        }
    }

	@Test
    public void testNormalMarshall() {
		byte[] keyBytes = HashUtils.getHashBytes("thiskey");
		byte[] valueBytes = "thisvalue".getBytes(StandardCharsets.US_ASCII);
		IMessage message = new Message(Status.GET, new K(keyBytes), new V(valueBytes));
		byte[] marshalledMessage = MessageMarshaller.marshall(message);
		IMessage unmarshalledMessage = MessageMarshaller.unmarshall(marshalledMessage);
		assertTrue(message.getStatus().equals(unmarshalledMessage.getStatus()));
		assertEquals(message.getK().getString(), unmarshalledMessage.getK().getString());
		assertEquals(message.getV().getString(), unmarshalledMessage.getV().getString());
         }

	@Test
    public void testNoKVMarshall() {
		IMessage message = new Message(Status.SERVER_STOPPED);
		byte[] marshalledMessage = MessageMarshaller.marshall(message);
		IMessage unmarshalledMessage = MessageMarshaller.unmarshall(marshalledMessage);
		assertTrue(message.getStatus().equals(unmarshalledMessage.getStatus()));
    }

    @Test
    public void testKeyMarshall() {
		byte[] keyBytes = HashUtils.getHashBytes("thiskey");
		IMessage message = new Message(Status.GET, new K(keyBytes));
		byte[] marshalledMessage = MessageMarshaller.marshall(message);
		IMessage unmarshalledMessage = MessageMarshaller.unmarshall(marshalledMessage);
		assertTrue(message.getStatus().equals(unmarshalledMessage.getStatus()));
		assertEquals(message.getK().getString(), unmarshalledMessage.getK().getString());
    }
}
