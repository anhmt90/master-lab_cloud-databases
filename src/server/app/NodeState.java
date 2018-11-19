package server.app;

public enum NodeState {
    /**
     * Default state when server is initialized
     */
    STOPPED,


    /**
     * Server has been started and is in service
     */
    STARTED,

    /**
     * Server is rearranging keys and does not serve PUT-requests
     */
    WRITE_LOCKED
    ;

}
