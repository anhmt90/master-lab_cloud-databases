package server.api;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.DatagramSocket;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.SocketTimeoutException;

import management.FailureReportMessage;
import management.FailureStatus;
import management.ConfigMessageMarshaller;
import server.app.Server;


public class HeartbeatReceiver implements Runnable{

	private static Logger LOG = LogManager.getLogger(Server.SERVER_LOG);
	
	private int missedHeartbeats;
	private int receiverPort;
	private Server server;

	private DatagramSocket socket;
	
	public HeartbeatReceiver(int receiverPort, Server server) {
		this.receiverPort = receiverPort;
		this.server = server;
	}
	
	public void run() {
		try {
			missedHeartbeats = 0;
			byte[] marshalledHeartbeat = ConfigMessageMarshaller.marshall(new FailureReportMessage(FailureStatus.HEARTBEAT));
			int heartbeatMessageLength = marshalledHeartbeat.length;
			
			socket = new DatagramSocket(receiverPort);
			socket.setSoTimeout(2 * server.getHeartbeatInterval());
			
			while(socket != null && !socket.isClosed()) {
				if(missedHeartbeats > 4) {
					server.reportFailure();
					return;
				}
				
				DatagramPacket heartbeat = new DatagramPacket(new byte[heartbeatMessageLength], heartbeatMessageLength);
				try{
					socket.receive(heartbeat);
					byte[] receivedHeartbeat = heartbeat.getData();
					FailureReportMessage heartbeatMessage = ConfigMessageMarshaller.unmarshallFailureReportMessage(receivedHeartbeat);
					if(heartbeatMessage.getStatus() == FailureStatus.HEARTBEAT) {
						LOG.debug("Received heartbeat from <" + heartbeat.getAddress() + ":" + heartbeat.getPort() + ">");
						missedHeartbeats = 0;
					}
				} catch (SocketTimeoutException ex) {
					missedHeartbeats++;
					LOG.warn("Didn't receive heartbeat. Timeout " + missedHeartbeats + " times");
				}
				
			}
		} catch (IOException ex) {
			LOG.error("Error trying to receive heartbeat.");
		} finally {
            close();
        }
		
	}
	
	public void close() {
		socket.close();
		socket = null;
	}
}
