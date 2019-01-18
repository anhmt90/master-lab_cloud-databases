package mapreduce;

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
        while (collector.isRunning() && isOpen) {
            try {
                OutputMessage outputMessage = receive();
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
            } catch (IOException e) {
                LOG.error(e);
                isOpen = false;
            }
        }

    }

    private void collectMapOutput(OutputMessage outputMessage) {
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
     * Receives a message sent by a client
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
