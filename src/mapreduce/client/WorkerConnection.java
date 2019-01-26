package mapreduce.client;

import client.mapreduce.Driver;
import client.mapreduce.OutputCollector;
import management.MessageSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocol.mapreduce.OutputMessage;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static protocol.Constants.MAX_ALLOWED_EOF;

public class WorkerConnection implements Runnable {
    private static Logger LOG = LogManager.getLogger(Driver.MAPREDUCE_LOG);

    private OutputCollector collector;
    private Socket workerSocket;

    private boolean isOpen;
    private BufferedInputStream bis;
    private BufferedOutputStream bos;

    public WorkerConnection(OutputCollector collector, Socket workerSocket) {
        this.collector = collector;
        this.workerSocket = workerSocket;
        isOpen = true;
    }

    @Override
    public void run() {
        int eofCounter = 0;
        while (collector.isRunning() && isOpen) {
            try {
                OutputMessage outputMessage = receive();
                if (outputMessage == null) {
                    eofCounter++;
                    if (eofCounter >= MAX_ALLOWED_EOF) {
                        LOG.warn("Got " + eofCounter + " successive EOF signals! Assume the other end has terminated but not closed the socket properly. " +
                                "Tear down connection now");
                        isOpen = false;
                    }
                    continue;
                }
                eofCounter = 0;
                switch (outputMessage.getTaskType()) {
                    case MAP:
                        collectMapOutput(outputMessage);
                        break;
                    case REDUCE:
                        collectReduceOutput(outputMessage);
                        break;
                    default:
                        throw new IllegalArgumentException("Undefined TaskType: " + outputMessage.getTaskType());
                }
                break;
            } catch (IOException e) {
                LOG.error(e);
                isOpen = false;
            }
        }

    }

    private void collectMapOutput(OutputMessage outputMessage) {
        LOG.info("Receive output: " + outputMessage.getOutputs());
        collector.getOutputCollection().putAll(outputMessage.getOutputs());
    }

    private void collectReduceOutput(OutputMessage outputMessage) {
        ConcurrentHashMap<String, String> finalOutputs = collector.getOutputCollection();

        HashMap<String, String> newOutput = outputMessage.getOutputs();
        for (Map.Entry<String, String> entry: newOutput.entrySet()) {
            if (! finalOutputs.containsKey(entry.getKey())) {
                finalOutputs.put(entry.getKey(), entry.getValue());
                continue;
            }

            String currVal = finalOutputs.get(entry.getKey());
            String newVal = currVal + " " + entry.getValue(); //TODO: escape by Json object
            finalOutputs.replace(entry.getKey(), newVal);
        }
    }

    /**
     * Receives a message sent by a worker
     *
     * @return the received message
     * @throws IOException
     */
    private OutputMessage receive() throws IOException {
        byte[] messageBuffer = new byte[1024 * 1024];
        while (true) {
            try {
                bis = new BufferedInputStream(workerSocket.getInputStream());
                int justRead = bis.read(messageBuffer);

                if (justRead < 0)
                    return null;

                OutputMessage message = MessageSerializer.deserialize(Arrays.copyOfRange(messageBuffer, 0, justRead));

                LOG.info("RECEIVE \t<"
                        + workerSocket.getInetAddress().getHostAddress() + ":"
                        + workerSocket.getPort() + ">: '"
                        + message.toString().trim() + "'");
                return message;
            } catch (EOFException e) {
                LOG.error("CATCH EOFException", e);
            }
        }
    }
}
