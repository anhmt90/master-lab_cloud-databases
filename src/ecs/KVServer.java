package ecs;

import management.ConfigMessage;
import management.ConfigStatus;

import java.util.function.Consumer;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;

import static util.FileUtils.SEP;
import static util.FileUtils.WORKING_DIR;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import util.HashUtils;

public class KVServer implements Comparable<KVServer> {
  private static final String ECS_LOG = "ECS";
  private static Logger LOG = LogManager.getLogger(ECS_LOG);

  public String getHashKey() {
    return hashKey;
  }

  private String hashKey;

  private InetSocketAddress address;
  private Socket socket;
  private int sshPort = 22;
  private String sshCMD;

  public KVServer(String host, String port) {
    this(host, Integer.parseInt(port));
  }

  public KVServer(String host, int port) {
    this(new InetSocketAddress(host, port));
  }

  public KVServer(InetSocketAddress address) {
    this.address = address;
    this.socket = new Socket();
    this.sshCMD = String.format("ssh -n %s -p %d nohup java -jar " + WORKING_DIR + SEP + "ms3-server.jar %d &",
        this.getHost(), sshPort,
        this.getPort());
    this.hashKey = HashUtils.getHash(String.format("%s:%d", this.getHost(), this.getPort()));
  }

  public void send(ConfigMessage msg) throws IOException {
    try {
      ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
      oos.writeObject(msg);
      oos.flush();
    } catch (IOException e) {
      e.printStackTrace();
    }
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
      LOG.info(String.format("Started server %s:%d via ssh", this.address.getHostString(), this.address.getPort()));
    } catch (IOException | InterruptedException e) {
      launched = false;
      LOG.error(String.format("Couldn't launch the server %s:%d", this.getHost(), this.getPort()));
    }
    callback.accept(launched);
  }

  void init(Metadata metadata, int cacheSize, String strategy) {
    ConfigMessage msg = new ConfigMessage(ConfigStatus.INIT, cacheSize, strategy, metadata);
    try {
      this.send(msg);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  void startServer() {
    ConfigMessage msg = new ConfigMessage(ConfigStatus.START);
    try {
      this.send(msg);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  void stopServer() {
    ConfigMessage msg = new ConfigMessage(ConfigStatus.STOP);
    try {
      this.send(msg);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  void shutDown() {
    ConfigMessage msg = new ConfigMessage(ConfigStatus.SHUTDOWN);
    try {
      this.send(msg);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  void lockWrite() {
    ConfigMessage msg = new ConfigMessage(ConfigStatus.LOCK_WRITE);
    try {
      this.send(msg);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  void unLockWrite() {
    ConfigMessage msg = new ConfigMessage(ConfigStatus.UNLOCK_WRITE);
    try {
      this.send(msg);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  void moveData(KeyHashRange range, KVServer anotherServer) {
    NodeInfo meta = new NodeInfo(anotherServer.getHost(), anotherServer.getPort(), range);
    ConfigMessage msg = new ConfigMessage(ConfigStatus.MOVE_DATA, meta);
    try {
      this.send(msg);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  void update(Metadata metadata) {
    ConfigMessage msg = new ConfigMessage(ConfigStatus.UPDATE_METADATA, metadata);
    try {
      this.send(msg);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public int compareTo(KVServer kvServer) {
    return this.getHashKey().compareTo(kvServer.getHashKey());
  }
}
