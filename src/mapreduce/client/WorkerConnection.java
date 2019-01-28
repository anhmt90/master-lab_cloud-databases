package mapreduce.client;

import management.MessageSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocol.mapreduce.StatusMessage;
import util.StringUtils;
import util.Validate;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static protocol.Constants.MAX_ALLOWED_EOF;

public class WorkerConnection implements Runnable {
    private static Logger LOG = LogManager.getLogger(Driver.MAPREDUCE_LOG);

    private StatusReceiver statusReceiver;
    private Socket workerSocket;

    private boolean isOpen;
    private BufferedInputStream bis;
    private BufferedOutputStream bos;

    public WorkerConnection(StatusReceiver statusReceiver, Socket workerSocket) {
        this.statusReceiver = statusReceiver;
        this.workerSocket = workerSocket;
        isOpen = true;
    }

    @Override
    public void run() {
        int eofCounter = 0;
        while (statusReceiver.isRunning() && isOpen) {
            try {
                StatusMessage resp = receive();
                if (resp == null) {
                    eofCounter++;
                    if (eofCounter >= MAX_ALLOWED_EOF) {
                        LOG.warn("Got " + eofCounter + " successive EOF signals! Assume the other end has terminated but not closed the socket properly. " +
                                "Tear down connection now");
                        isOpen = false;
                    }
                    continue;
                }
                eofCounter = 0;
                if(!resp.isSuccess())
                    LOG.warn("Worker at " + workerSocket.getRemoteSocketAddress() + " reports failure");

                ConcurrentHashMap<String, String> outputHashMap = statusReceiver.getOutputCollection();
                for(Object keyObj : resp.getKeySet()){
                    Validate.isTrue(keyObj instanceof String, "keyObj is not String");
                    String key = String.valueOf(keyObj);
                    outputHashMap.put(key, StringUtils.EMPTY_STRING);
                }
                break;
            } catch (IOException e) {
                LOG.error(e);
                isOpen = false;
            }
        }

    }

//     switch (outputMessage.getTaskType()) {
//        case MAP:
//            collectMapOutput(outputMessage);
//            break;
//        case REDUCE:
//            collectReduceOutput(outputMessage);
//            break;
//        default:
//            throw new IllegalArgumentException("Undefined TaskType: " + outputMessage.getTaskType());
//    }

//    private void collectMapOutput(StatusMessage outputMessage) {
//        LOG.info("Receive output: " + outputMessage.getOutputs());
//        statusReceiver.getOutputCollection().putAll(outputMessage.getOutputs());
//    }
//
//    private void collectReduceOutput(StatusMessage outputMessage) {
//        ConcurrentHashMap<String, String> finalOutputs = statusReceiver.getOutputCollection();
//
//        HashMap<String, String> newOutput = outputMessage.getOutputs();
//        for (Map.Entry<String, String> entry: newOutput.entrySet()) {
//            if (! finalOutputs.containsKey(entry.getKey())) {
//                finalOutputs.put(entry.getKey(), entry.getValue());
//                continue;
//            }
//
//            String currVal = finalOutputs.get(entry.getKey());
//            String newVal = currVal + " " + entry.getValue(); //TODO: escape by Json object
//            finalOutputs.replace(entry.getKey(), newVal);
//        }
//    }

    /**
     * Receives a message sent by a worker
     *
     * @return the received message
     * @throws IOException
     */
    private StatusMessage receive() throws IOException {
        byte[] messageBuffer = new byte[1024 * 1024];
        while (true) {
            try {
                bis = new BufferedInputStream(workerSocket.getInputStream());
                int justRead = bis.read(messageBuffer);

                if (justRead < 0)
                    return null;

                StatusMessage message = MessageSerializer.deserialize(Arrays.copyOfRange(messageBuffer, 0, justRead));

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
