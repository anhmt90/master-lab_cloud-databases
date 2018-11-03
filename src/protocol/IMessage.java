package protocol;

import java.util.Arrays;

public interface IMessage {

    enum Status {
        GET(0x30),            /* Get - request */
        GET_ERROR(0x31),        /* requested tuple (i.e. value) not found */
        GET_SUCCESS(0x32),    /* requested tuple (i.e. value) found */
        PUT(0x33),            /* Put - request */
        PUT_SUCCESS(0x34),    /* Put - request successful, tuple inserted */
        PUT_UPDATE(0x35),    /* Put - request successful, i.e. value updated */
        PUT_ERROR(0x36),        /* Put - request not successful */
        DELETE(0x37),        /* Delete - request */
        DELETE_SUCCESS(0x38), /* Delete - request successful */
        DELETE_ERROR(0x39)    /* Delete - request successful */;

        byte code;


        Status(int code) {
            this.code = (byte) code;
        }

        public byte getCode() {
            return this.code;
        }

        public static Status getByCode(byte code){
            final Status[] all = Status.values();
            return all[code - all[0].getCode()];
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            K k = (K) o;
            return Arrays.equals(key, k.key);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(key);
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


