package management;

public interface IExternalConfigurationService {
    boolean initKVServer(String metadata, int cacheSize, String strategy);

    boolean stopService();
    boolean startService();
    boolean shutdown();

    void lockWrite();
    void unlockWrite();
    void update(String metadata);
}

