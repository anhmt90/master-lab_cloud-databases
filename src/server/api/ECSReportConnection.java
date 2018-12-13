package server.api;

import management.FailureReportMessage;
import management.ConfigMessage;
import management.ConfigMessageMarshaller;
import management.ConfigStatus;
import management.FailureStatus;


import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import ecs.ExternalConfigurationService;
import ecs.NodeInfo;
import server.app.Server;
import util.LogUtils;

import static protocol.IMessage.MAX_MESSAGE_LENGTH;

public class ECSReportConnection {

    private static Logger LOG = LogManager.getLogger(Server.SERVER_LOG);
        
    private Socket socket;

    private BufferedOutputStream bos;    
    private BufferedInputStream bis;
	
	private Server server;
	
	
	public ECSReportConnection(Server server) throws IOException {
		try {
            socket = new Socket();
            socket.connect(new InetSocketAddress(ExternalConfigurationService.ECS_ADDRESS, ExternalConfigurationService.REPORT_PORT), 5000);
        } catch (UnknownHostException uhe) {
            throw LogUtils.printLogError(LOG, uhe, "Unknown host");
        } catch (SocketTimeoutException ste) {
            throw LogUtils.printLogError(LOG, ste, "Could not connect to ECS. Connection timeout.");
        } catch (IOException ioe) {
            throw LogUtils.printLogError(LOG, ioe, "Could not connect to ECS.");
        }
		this.server = server;
	}
	
	public void send(FailureReportMessage message) throws IOException {
        try {
            bos = new BufferedOutputStream(socket.getOutputStream());
            byte[] bytes = ConfigMessageMarshaller.marshall(message);
            bos.write(bytes);
            bos.flush();

            LOG.info("SEND \t<"
                    + socket.getInetAddress().getHostAddress() + ":"
                    + socket.getPort() + ">: '"
                    + message.toString() + "'");

        } catch (IOException e) {
            LOG.error(e);
            throw e;
        }
    }
	
	private FailureReportMessage receive() throws IOException {
        byte[] messageBuffer = new byte[MAX_MESSAGE_LENGTH];
        while (true) {
            try {
                bis = new BufferedInputStream(socket.getInputStream());
                int justRead = bis.read(messageBuffer);
                FailureReportMessage message = ConfigMessageMarshaller.unmarshallFailureReportMessage(Arrays.copyOfRange(messageBuffer, 0, justRead));

                LOG.info("RECEIVE \t<"
                        + socket.getInetAddress().getHostAddress() + ":"
                        + socket.getPort() + ">: '"
                        + message.toString().trim() + "'");
                return message;
            } catch (EOFException e) {
                LOG.error("CATCH EOFException", e);
            }
        }
    }
	
	private boolean sendAndExpect(FailureReportMessage toSend, FailureStatus expected) {
        try {
            send(toSend);
            FailureReportMessage response = receive();
            return response.getStatus().equals(expected);
        } catch (IOException e) {
            LOG.error("Error! ", e);
            return false;
        }
    }
	
	
	public boolean sendFailureReport(NodeInfo failedServer) {
		FailureReportMessage failureMessage = new FailureReportMessage(FailureStatus.SERVER_FAILURE, failedServer);
		return sendAndExpect(failureMessage, FailureStatus.FAILURE_RESOLVED);
	}
}
