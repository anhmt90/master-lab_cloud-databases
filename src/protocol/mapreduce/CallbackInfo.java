package protocol.mapreduce;

import java.io.Serializable;

public class CallbackInfo implements Serializable {
    private String responseAddress;
    private int responsePort;

    public CallbackInfo(String responseAddress, int responsePort) {
        this.responseAddress = responseAddress;
        this.responsePort = responsePort;
    }

    public String getResponseAddress() {
        return responseAddress;
    }

    public int getResponsePort() {
        return responsePort;
    }

    @Override
    public String toString() {
        return "CallbackInfo{" +
                "responseAddress='" + responseAddress + '\'' +
                ", responsePort=" + responsePort +
                '}';
    }
}
