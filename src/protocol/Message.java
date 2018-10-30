package protocol;

public class Message implements IMessage{

    private class Content {
        String key;
        String value;

        public Content(String key, String value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public String getValue() {
            return value;
        }
    }

    @Override
    public String getKey() {
        return null;
    }

    @Override
    public String getValue() {
        return null;
    }

    @Override
    public StatusType getStatus() {
        return null;
    }
}
