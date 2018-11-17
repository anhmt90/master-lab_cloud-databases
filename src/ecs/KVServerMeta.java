package ecs;

public class KVServerMeta {
  private String host;
  private int port;
  private KeyHashRange range;

  public KVServerMeta(String host, int port, String start, String end) {
    this.host = host;
    this.port = port;
    this.range = new KeyHashRange(start, end);
  }
}
