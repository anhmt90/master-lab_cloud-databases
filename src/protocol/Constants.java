package protocol;

import protocol.kv.K;
import protocol.kv.V;

public class Constants {
    /**
     * 1 byte indicating message status
     * 1 byte indicating whether batch data or not
     * 3 bytes indicating the length of {@link V} in byte
     * 16 bytes contains the MD5 hash of {@link K}
     * 1024 * 120 bytes are the actual data of V
     */
    public static final int MAX_KV_MESSAGE_LENGTH = 1 + 1 + 3 + 16 + 1024 * 120;

    public static final int MR_TASK_RECEIVER_PORT_DISTANCE = -1000;

    public static final int MR_TASK_HANDLER_PORT_DISTANCE = -2000;

    public static final int MAX_TASK_MESSAGE_LENGTH = 63 * 1024;

    public static final int MAX_ALLOWED_EOF = 3;

    public static final int MAX_BUFFER_LENGTH = 1024 * 1024; // 1 MB
}
