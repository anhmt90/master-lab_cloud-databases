package mapreduce.common;

import protocol.kv.IMessage;
import util.StringUtils;

public enum ApplicationID  {
    WORD_COUNT("wc"),
    INVERTED_INDEX("ii")
    ;

    String id;

    ApplicationID(String id) {
        this.id = id;
    }
    public String getId() {
        return this.id;
    }
}
