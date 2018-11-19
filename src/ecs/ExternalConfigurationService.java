package ecs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import server.storage.cache.CacheDisplacementStrategy;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static util.StringUtils.WHITE_SPACE;

public class ExternalConfigurationService extends Thread implements IECS {
  public static final String ECS_LOG = "ECS";
  private static final int ECS_PORT = 65432;
  private static Logger LOG = LogManager.getLogger(ECS_LOG);

  private ServerSocket serverSocket;
  private List<KVServer> idleNodes = new ArrayList<>();
  private List<KVServer> activeNodes = new ArrayList<>();
  private Metadata metadata;
  private boolean running;

  private void updateMetadata() {
    Metadata md = new Metadata();

    for (int i = 0; i < this.activeNodes.size(); i++) {
      KVServer start = this.activeNodes.get(i);
      KVServer end = this.activeNodes.get((i + 1) % this.activeNodes.size());

      md.add(end.getHost(), end.getPort(), start.getHashKey(), end.getHashKey());
    }

    this.metadata = md;
  }

  private void publishMetada() {
    for (KVServer kvS: this.activeNodes) {
      kvS.update(this.getMetadata());
    }
  }

  private void updateActiveRing() {
    Collections.sort(this.activeNodes);
    updateMetadata();
  }

  public Metadata getMetadata() {
    return this.metadata;
  }

  @Override
  public void initService(int numberOfNodes, int cacheSize, String displacementStrategy) {
    numberOfNodes = idleNodes.size() < numberOfNodes? idleNodes.size() : numberOfNodes;
    for (int i = 0; i < numberOfNodes; i++)  {
      int n = ThreadLocalRandom.current().nextInt(idleNodes.size());
      KVServer kvS = idleNodes.get(n);
      idleNodes.remove(n);
      activeNodes.add(kvS);
    }

    updateActiveRing();

    for (KVServer kvS: this.activeNodes) {
      kvS.init(this.getMetadata(), cacheSize, displacementStrategy);
    }
  }

  @Override
  public void startService() {
    for (KVServer kvS: this.activeNodes) {
      kvS.startServer();
    }
  }

  @Override
  public void stopService() {
    for (KVServer kvS: this.activeNodes) {
      kvS.stopServer();
      this.idleNodes.add(kvS);
    }
    this.activeNodes.clear();
    this.updateActiveRing();
  }

  @Override
  public void shutDown() {
    for (KVServer kvS: this.activeNodes) {
      kvS.shutDown();
    }

    for (KVServer kvS: this.idleNodes) {
      kvS.shutDown();
    }

    this.running = false;
    try {
      this.serverSocket.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void addNode(int cacheSize, String displacementStrategy) {
    int nodeToRun = ThreadLocalRandom.current().nextInt(this.idleNodes.size());
    KVServer kvServer = this.idleNodes.get(nodeToRun);
    this.idleNodes.remove(kvServer);
    this.activeNodes.add(kvServer);

    this.updateActiveRing();
    kvServer.init(this.getMetadata(), cacheSize, displacementStrategy);
    int idx = this.activeNodes.indexOf(kvServer);
    KVServer predecessor = this.activeNodes.get(idx == 0? this.activeNodes.size() - 1 : idx - 1);
    KVServer successor = this.activeNodes.get((idx + 1) % this.activeNodes.size());
    successor.lockWrite();
    successor.moveData(new KeyHashRange(predecessor.getHashKey(), kvServer.getHashKey()), kvServer);
    successor.unLockWrite();

    kvServer.startServer();
    publishMetada();
  }

  @Override
  public void removeNode() {
    int idx = ThreadLocalRandom.current().nextInt(this.activeNodes.size());
    KVServer kvS = this.activeNodes.get(idx);


    KVServer successor = this.activeNodes.get((idx + 1) % this.activeNodes.size());
    KVServer predecessor = this.activeNodes.get(idx == 0? this.activeNodes.size() - 1 : idx - 1);
    kvS.lockWrite();
    successor.lockWrite();
    kvS.moveData(new KeyHashRange(predecessor.getHashKey(), kvS.getHashKey()), successor);
    successor.unLockWrite();
    kvS.stopServer();

    this.activeNodes.remove(kvS);
    this.idleNodes.add(kvS);
    updateActiveRing();
    publishMetada();
  }

  @Override
  public void run() {
    this.running = true;

    if (serverSocket != null) {
      while (this.running) {
        try {
          Socket client = serverSocket.accept();
          KVServer kvS = new KVServer(client);
          this.idleNodes.add(kvS);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public ExternalConfigurationService(String configFile) throws IOException {
    List<String> lines = null;
    try {
      lines = Files.readAllLines(Paths.get(configFile));
    } catch (IOException e) {
      e.printStackTrace();
    }
    this.serverSocket = new ServerSocket(ECS_PORT);

    for (String line: lines) {
      String[] serverParams = line.split(WHITE_SPACE);
      String serverHost = serverParams[1];
      String serverPort = serverParams[2];
      KVServer kvS = null;
      try {
        kvS = new KVServer(serverHost, serverPort);
        kvS.launch();
      } catch (IOException | InterruptedException | NoSuchAlgorithmException e) {
        e.printStackTrace();
      }
    }

    this.start();
  }

}
