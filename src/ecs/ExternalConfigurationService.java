package ecs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import server.storage.cache.CacheDisplacementStrategy;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class ExternalConfigurationService extends Thread implements IECS {
  public static final String ECS_LOG = "ECS";
  private static Logger LOG = LogManager.getLogger(ECS_LOG);

  private ServerSocket serverSocker;
  private List<KVServer> idle = new ArrayList<>();
  private List<KVServer> active = new ArrayList<>();
  private Metadata metadata;

  private void updateMetadata() {
    Metadata md = new Metadata();

    for (int i = 0; i < this.active.size(); i++) {
      KVServer start = this.active.get(i);
      KVServer end = this.active.get((i + 1) % this.active.size());

      md.add(end.getHost(), end.getPort(), start.getHashKey(), end.getHashKey());
    }

    this.metadata = md;
  }

  private void publishMetada() {
    for (KVServer kvS: this.active) {
      kvS.update(this.getMetadata());
    }
  }

  private void updateActiveRing() {
    Collections.sort(this.active);
    updateMetadata();
  }

  public Metadata getMetadata() {
    return this.metadata;
  }

  @Override
  public void initService(int numberOfNodes, int cacheSize, CacheDisplacementStrategy displacementStrategy) {
    numberOfNodes = idle.size() < numberOfNodes? idle.size() : numberOfNodes;
    for (int i = 0; i < numberOfNodes; i++)  {
      int n = ThreadLocalRandom.current().nextInt(idle.size());
      KVServer kvS = idle.get(n);
      idle.remove(n);
      active.add(kvS);
    }

    updateActiveRing();

    for (KVServer kvS: this.active) {
      kvS.init(this.getMetadata(), cacheSize, displacementStrategy);
    }
  }

  @Override
  public void startService() {
    for (KVServer kvS: this.active) {
      kvS.startServer();
    }
  }

  @Override
  public void stopService() {
    for (KVServer kvS: this.active) {
      kvS.stopServer();
      this.idle.add(kvS);
    }
    this.active.clear();
    this.updateActiveRing();
  }

  @Override
  public void shutDown() {
  }

  @Override
  public void addNode(int cacheSize, CacheDisplacementStrategy displacementStrategy) {
    int nodeToRun = ThreadLocalRandom.current().nextInt(this.idle.size());
    KVServer kvS = this.idle.get(nodeToRun);
    this.idle.remove(kvS);
    this.active.add(kvS);

    this.updateActiveRing();
    kvS.init(this.getMetadata(), cacheSize, displacementStrategy);
    int idx = this.active.indexOf(kvS);
    KVServer successor = this.active.get((idx + 1) % this.active.size());
    KVServer predecessor = this.active.get(idx == 0? this.active.size() - 1 : idx - 1);
    successor.lockWrite();
    successor.moveData(new KeyHashRange(predecessor.getHashKey(), kvS.getHashKey()), kvS);
    successor.unLockWrite();

    kvS.startServer();
    publishMetada();
  }

  @Override
  public void removeNode() {
    int idx = ThreadLocalRandom.current().nextInt(this.active.size());
    KVServer kvS = this.active.get(idx);


    KVServer successor = this.active.get((idx + 1) % this.active.size());
    KVServer predecessor = this.active.get(idx == 0? this.active.size() - 1 : idx - 1);
    kvS.lockWrite();
    successor.lockWrite();
    kvS.moveData(new KeyHashRange(predecessor.getHashKey(), kvS.getHashKey()), successor);
    successor.unLockWrite();
    kvS.stopServer();

    this.active.remove(kvS);
    this.idle.add(kvS);
    updateActiveRing();
    publishMetada();
  }

  public ExternalConfigurationService(int port, String configFile) {
    List<String> lines = null;
    try {
      lines = Files.readAllLines(Paths.get(configFile));
    } catch (IOException e) {
      e.printStackTrace();
    }

    for (String line: lines) {
      String[] serverParams = line.split(" ");
      String serverHost = serverParams[0];
      String serverPort = serverParams[1];
      KVServer kvS = null;
      try {
        kvS = new KVServer(serverHost, serverPort);
      } catch (IOException | InterruptedException | NoSuchAlgorithmException e) {
        e.printStackTrace();
      }
      this.idle.add(kvS);
    }
  }

}
