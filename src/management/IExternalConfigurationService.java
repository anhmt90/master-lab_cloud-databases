package management;

public interface IExternalConfigurationService {
    void initKVServer();
    boolean stopService();
    boolean startService();
    boolean shutdown();

    void lockWrite();
    void unlockWrite();
    void update(String metadata);
}

