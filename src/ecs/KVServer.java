package ecs;

import management.ConfigMessage;
import management.ConfigMessageMarshaller;
import management.ConfigStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import util.HashUtils;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static protocol.IMessage.MAX_MESSAGE_LENGTH;

/**
 * Handles connection from ECS to one key-value storage server
 *
 */
public class KVServer implements Comparable<KVServer> {
  private static final String ECS_LOG = "ECS";
  private static Logger LOG = LogManager.getLogger(ECS_LOG);

  public String getHashKey() {
    return hashKey;
  }

  private String hashKey;

  private String nodeName;
  private InetSocketAddress address;
  private Socket socket;
  private BufferedInputStream bis;
  private BufferedOutputStream bos;

  private final static int SSH_PORT = 22;
  private String[] sshCMD;

  private final static int RETRY_NUM = 5;
  private final static int RETRY_TIME = 1000; // milliseconds

  public KVServer(String serverName, String host, String port) {
    this(serverName, host, Integer.parseInt(port));
  }

  public KVServer(String serverName, String host, int port) {
    this(serverName, new InetSocketAddress(host, port));
  }

  public KVServer(String serverName, InetSocketAddress address) {
    this.nodeName = serverName;
    this.address = address;
    this.socket = new Socket();
    String[] cmds = {"ssh", "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
        "ecs@"+getHost(), "-p", ""+SSH_PORT,
        "nohup java -jar /opt/app/ms3-server.jar " + getNodeName() + " " + getPort() +" > /opt/app/logs/server_docker & "
    };
    this.sshCMD = cmds;
    this.hashKey = HashUtils.getHash(String.format("%s:%d", this.getHost(), this.getPort()));
  }

  public String getHost() {
    return this.address.getHostString();
  }

  public int getPort() {
    return this.address.getPort();
  }

  void launch(Consumer<Boolean> callback) {
    Process proc;
    Runtime run = Runtime.getRuntime();
    boolean launched = true;
    try {
      proc = run.exec(this.sshCMD);
      proc.waitFor();
      BufferedReader in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
      String line;
      while ((line = in.readLine()) != null) {
        LOG.info("SSH response: " + line);
      }

      LOG.debug("Initializing socket");
      this.initSocket();
      this.bos.write(new byte[]{1});
      this.bos.flush();
      LOG.info(String.format("Started server %s:%d via ssh", this.address.getHostString(), this.address.getPort()));
    } catch (IOException | InterruptedException e) {
      launched = false;
      LOG.error(String.format("Couldn't launch the server %s:%d", this.getHost(), this.getPort()));
      LOG.error(e);
    }
    callback.accept(launched);
  }


  /**
   * Sends a message to the connected server
   * 
   * @param message message to be sent
   * @throws IOException
   */
  public void send(ConfigMessage message) throws IOException {
    try {
      bos.write(ConfigMessageMarshaller.marshall(message));
      bos.flush();
      LOG.info("SEND \t<"
          + socket.getInetAddress().getHostAddress() + ":"
          + socket.getPort() + ">: '"
          + message.toString() + "'");

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Receives a message sent by {@link server.app.Server}
   *
   * @return the received message
   * @throws IOException
   */
  private ConfigMessage receive() throws IOException {
    byte[] messageBuffer = new byte[MAX_MESSAGE_LENGTH];
    int bytesCopied = bis.read(messageBuffer);
    LOG.info("Read " + bytesCopied + " from input stream");

    ConfigMessage message = ConfigMessageMarshaller.unmarshall(messageBuffer);

    LOG.info("RECEIVE \t<"
        + socket.getInetAddress().getHostAddress() + ":"
        + socket.getPort() + ">: '"
        + message.toString().trim() + "'");

    return message;
  }

  boolean init(Metadata metadata, int cacheSize, String strategy) {
    ConfigMessage msg = new ConfigMessage(ConfigStatus.INIT, cacheSize, strategy, metadata);
    try {
      return sendAndExpect(msg, ConfigStatus.INIT_SUCCESS);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return false;
  }

  boolean startServer() {
    ConfigMessage msg = new ConfigMessage(ConfigStatus.START);
    try {
      return sendAndExpect(msg, ConfigStatus.START_SUCCESS);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return false;
  }

  boolean stopServer() {
    ConfigMessage msg = new ConfigMessage(ConfigStatus.STOP);
    try {
      return sendAndExpect(msg, ConfigStatus.STOP_SUCCESS);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return false;
  }

  boolean shutDown() {
    ConfigMessage msg = new ConfigMessage(ConfigStatus.SHUTDOWN);
    try {
      return sendAndExpect(msg, ConfigStatus.SHUTDOWN_SUCCESS);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return false;
  }

  boolean lockWrite() {
    ConfigMessage msg = new ConfigMessage(ConfigStatus.LOCK_WRITE);
    try {
      return sendAndExpect(msg, ConfigStatus.LOCK_WRITE_SUCCESS);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return false;
  }

  boolean unLockWrite() {
    ConfigMessage msg = new ConfigMessage(ConfigStatus.UNLOCK_WRITE);
    try {
      return sendAndExpect(msg, ConfigStatus.UNLOCK_WRITE_SUCCESS);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return false;
  }

  boolean moveData(KeyHashRange range, KVServer target) {
    NodeInfo meta = new NodeInfo(target.getNodeName(), target.getHost(), target.getPort(), range);
    ConfigMessage msg = new ConfigMessage(ConfigStatus.MOVE_DATA, meta);
    try {
      return sendAndExpect(msg, ConfigStatus.MOVE_DATA_SUCCESS);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return false;
  }

  boolean update(Metadata metadata) {
    ConfigMessage msg = new ConfigMessage(ConfigStatus.UPDATE_METADATA, metadata);
    try {
      return sendAndExpect(msg, ConfigStatus.UPDATE_METADATA_SUCCESS);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return false;
  }

  /**
   * Sends a ConfigMessage and checks if the server response matches an expected response
   * 
   * @param toSend message to be sent to the server
   * @param expected status of the expected server response
   * @return true if server response matches the expected one
   * @throws IOException
   */
  private boolean sendAndExpect(ConfigMessage toSend, ConfigStatus expected) throws IOException {
    if (bos != null && bis != null) {
      send(toSend);
      ConfigMessage response = receive();
      if (response.getStatus().equals(expected))
        return true;
    }
    return false;
  }

  private void initSocket() throws IOException {
    LOG.info("Connecting to the server");
    for (int i = 0; i < RETRY_NUM; i++) {
      try {
        socket = new Socket();
        TimeUnit.MILLISECONDS.sleep(RETRY_TIME);
        socket.connect(this.address, 5000);
        socket.setSoTimeout(5000);
        break;
      } catch (IOException | InterruptedException e) {
        LOG.info(String.format("Couldn't connect trying again (%d/%d)...", i+1, RETRY_NUM));
      }
    }
    LOG.info("Connected");
    bos = new BufferedOutputStream(socket.getOutputStream());
    bis = new BufferedInputStream(socket.getInputStream());
  }

  public void setNodeName(String nodeName) {
    this.nodeName = nodeName;
  }

  public String getNodeName() {
    return nodeName;
  }

  @Override
  public int compareTo(KVServer kvServer) {
    return this.getHashKey().compareTo(kvServer.getHashKey());
  }
}
