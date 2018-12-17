package management;

import ecs.Metadata;

public interface IExternalConfigurationService {
    boolean initKVServer(Metadata metadata, int cacheSize, String strategy);

    boolean stopService();
    boolean startService();
    boolean shutdown();

    boolean lockWrite();
    boolean unlockWrite();
    boolean update(Metadata metadata);
}

