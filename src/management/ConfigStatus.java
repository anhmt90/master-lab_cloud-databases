package management;

public enum ConfigStatus {
        INIT,
        START,
        STOP,
        SHUTDOWN,

        INIT_SUCCESS,
        START_SUCCESS,
        STOP_SUCCESS,
        SHUTDOWN_SUCCESS,

        INIT_ERRO,
        START_ERROR,
        STOP_ERROR,
        SHUTDOWN_ERROR,

        NO_TRANSITION
}
