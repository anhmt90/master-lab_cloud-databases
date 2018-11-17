package management;

public enum ConfigStatus {
        INIT,
        START,
        STOP,
        SHUTDOWN,
        LOCKWRITE,
        UNLOCKWRITE,
        MOVEDATA,
        UPDATEMETA,

        INIT_SUCCESS,
        START_SUCCESS,
        STOP_SUCCESS,
        SHUTDOWN_SUCCESS,
        LOCKWRITE_SUCCESS,
        UNLOCKWRITE_SUCCESS,
        MOVEDATA_SUCCESS,
        UPDATEMETA_SUCCESS,

        INIT_ERRO,
        START_ERROR,
        STOP_ERROR,
        SHUTDOWN_ERROR,
        LOCKWRITE_ERROR,
        UNLOCKWRITE_ERROR,
        MOVEDATA_ERROR,
        UPDATEMETA_ERROR,

        NO_TRANSITION
}
