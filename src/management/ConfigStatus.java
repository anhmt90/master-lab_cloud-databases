package management;

public enum ConfigStatus {
        INIT,
        START,
        STOP,
        SHUTDOWN,
        LOCK_WRITE,
        UNLOCK_WRITE,
        MOVE_DATA,
        UPDATE_METADATA,

        BOOT_SUCCESS, /*Reply the SSH command*/
        INIT_SUCCESS,
        START_SUCCESS,
        STOP_SUCCESS,
        SHUTDOWN_SUCCESS,
        LOCK_WRITE_SUCCESS,
        UNLOCK_WRITE_SUCCESS,
        MOVE_DATA_SUCCESS,
        UPDATE_METADATA_SUCCESS,

        ERROR
}
