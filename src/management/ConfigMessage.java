package management;

import java.io.Serializable;

public class ConfigMessage implements Serializable {
    ConfigStatus status;

    public ConfigMessage(ConfigStatus adminStatus) {
        this.status = adminStatus;
    }

    public ConfigStatus getStatus() {
        return status;
    }
}
