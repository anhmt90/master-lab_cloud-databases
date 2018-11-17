package ecs;

import server.storage.cache.CacheDisplacementStrategy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;

import static util.FileUtils.SEP;
import static util.FileUtils.WORKING_DIR;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import util.HashUtils;

public class KVServer implements Runnable, Comparable<KVServer> {
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

  public KVServer(String host, String port) throws IOException, InterruptedException, NoSuchAlgorithmException {
    this(host, Integer.parseInt(port));
  }

  public KVServer(String host, int port) throws IOException, InterruptedException, NoSuchAlgorithmException {
    this.address = new InetSocketAddress(host, port);
    this.socket = new Socket();
    this.sshCMD = String.format("ssh -n %s -p %d nohup java -jar " + WORKING_DIR + SEP + "ms3-server.jar %d &",
        host, sshPort,
        port);
    this.hashKey = HashUtils.getHash(String.format("%s:%d", host, port));
    launch();
  }

  public String getHost() {
    return this.address.getHostString();
  }

  public int getPort() {
    return this.address.getPort();
  }

  void launch() throws IOException, InterruptedException {
    Process proc;
    Runtime run = Runtime.getRuntime();
    proc = run.exec(this.sshCMD);
    proc.waitFor();
    LOG.info(String.format("Started server %s:%d via ssh", this.address.getHostString(), this.address.getPort()));
  }

  void init(Metadata metadata, int cacheSize, CacheDisplacementStrategy strategy) {
  }

  void startServer() {
  }

  void stopServer() {
  }

  void shutDown() {
  }

  void lockWrite() {
  }

  void unLockWrite() {
  }

  void moveData(KeyHashRange range, KVServer anotherServer) {
  }

  void update(Metadata metadata) {
  }

  @Override
  public void run() {
    this.launch();
  }

  @Override
  public int compareTo(KVServer kvServer) {
    return this.getHashKey().compareTo(kvServer.getHashKey());
  }
}
