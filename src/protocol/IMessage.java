package protocol;

public interface IMessage {

    enum Status {
        GET, 			/* Get - request */
        GET_ERROR, 		/* requested tuple (i.e. value) not found */
        GET_SUCCESS, 	/* requested tuple (i.e. value) found */
        PUT, 			/* Put - request */
        PUT_SUCCESS, 	/* Put - request successful, tuple inserted */
        PUT_UPDATE, 	/* Put - request successful, i.e. value updated */
        PUT_ERROR, 		/* Put - request not successful */
        DELETE, 		/* Delete - request */
        DELETE_SUCCESS, /* Delete - request successful */
        DELETE_ERROR 	/* Delete - request successful */
    }

    class K {
        String key;

        public K(String key) {
            this.key = key;
        }

        public String get() {
            return key;
        }
    }

    class V {
        String value;

        public V(String value) {
            this.value = value;
        }

        public String get()  {
            return value;
        }
    }

    /**
     * @return the key that is associated with this message,
     * null if not key is associated.
     */
    public K getKey();

    /**
     * @return the value that is associated with this message,
     * null if not value is associated.
     */
    public V getValue();

    /**
     * @return a status string that is used to identify request types,
     * response types and error types associated to the message.
     */
    public Status getStatus();

}


