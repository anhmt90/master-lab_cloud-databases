package management;

public class ConfigMessage implements IConfigMessage {
    ConfigStatus status;

    public ConfigMessage(ConfigStatus adminStatus) {
        this.status = adminStatus;
    }
}
