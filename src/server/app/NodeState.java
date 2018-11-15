package server.app;

public enum NodeState {
    /**
     * Default state when server is initialized
     */
    STOPPED,

    /**
     * Server is stopping but still has to finish ongoing requests
     */
    STOPPING,

    /**
     * Server is starting the storage service
     */
    STARTING,

    /**
     * Server has been started and is in service
     */
    RUNNING
    ;

}
