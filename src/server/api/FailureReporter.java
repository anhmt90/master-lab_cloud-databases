package server.api;

import management.FailureReportMessage;
import management.ConfigMessageMarshaller;
import management.ReportStatus;


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

/**
 * Establishes a connection with the ECS and then sends a failure report to it
 * 
 */
public class FailureReporter {
    private static Logger LOG = LogManager.getLogger(Server.SERVER_LOG);
    public static final int REPORT_PORT = 54321;
    public static final String ECS_ADDRESS = "127.0.0.1";

    private Socket socket;
    private BufferedOutputStream bos;
    private BufferedInputStream bis;
	

	
	public FailureReporter() throws IOException {
		try {
            socket = new Socket();
            socket.connect(new InetSocketAddress(ECS_ADDRESS, REPORT_PORT), 5000);
            LOG.info("Connection to failure report portal established!");
        } catch (UnknownHostException e) {
            throw LogUtils.printLogError(LOG, e, "Unknown host");
        } catch (SocketTimeoutException e) {
            throw LogUtils.printLogError(LOG, e, "Could not connect to ECS. Connection timeout.");
        } catch (IOException e) {
            throw LogUtils.printLogError(LOG, e, "Could not connect to ECS.");
        }
	}

	/**
	 * Sends the failure report
	 * 
	 * @param failedServer the server that has failed
	 * @return true if failure was successfully handled
	 */
    public boolean sendFailureReport(NodeInfo failedServer) {
        FailureReportMessage reportMessage = new FailureReportMessage(ReportStatus.SERVER_FAILED, failedServer);
        LOG.info("Sending message " + reportMessage + " to ECS");

        return sendAndExpect(reportMessage, ReportStatus.REPORT_RECEIVED);
    }

    private boolean sendAndExpect(FailureReportMessage toSend, ReportStatus expected) {
        try {
            send(toSend);
            FailureReportMessage response = receive();
            return response.getStatus().equals(expected);
        } catch (IOException e) {
            LOG.error("Error! ", e);
            return false;
        }
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
	
}
