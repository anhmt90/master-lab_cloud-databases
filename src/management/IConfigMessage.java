package management;

public interface IConfigMessage {
    enum ConfigStatus {
        START,
        STOP,
        SHUTDOWN,

        START_SUCCESS,
        STOP_SUCCESS,
        SHUTDOWN_SUCCESS,

        START_ERROR,
        STOP_ERROR,
        SHUTDOWN_ERROR
    }
}

