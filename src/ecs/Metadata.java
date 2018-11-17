package ecs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Metadata implements Serializable {
  List<KVServerMeta> meta = new ArrayList<>();

  public void add(String host, int port, String start, String end) {
    KVServerMeta kvSMeta = new KVServerMeta(host, port, start, end);
    this.meta.add(kvSMeta);
  }
}
