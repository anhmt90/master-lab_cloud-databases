package protocol;

import java.io.Serializable;

import ecs.Metadata;

public interface IMessage extends Serializable {
    /**
     * 1 byte indicating message status
     * 1 byte indicating whether batch data or not
     * 3 bytes indicating the length of {@link V} in byte
     * 16 bytes contains the MD5 hash of {@link K}
     * 1024 * 120 bytes are the actual data of V
     */
    public static final int MAX_MESSAGE_LENGTH = 1 + 1 + 3 + 16 + 1024 * 120;
    enum Status {
        GET(0x01),            /* Get - request */
        GET_ERROR(0x02),        /* requested tuple (i.e. value) not found */
        GET_SUCCESS(0x03),    /* requested tuple (i.e. value) found */
        PUT(0x04),            /* Put - request */
        PUT_SUCCESS(0x05),    /* Put - request successful, tuple inserted */
        PUT_UPDATE(0x06),    /* Put - request successful, i.e. value updated */
        PUT_ERROR(0x07),        /* Put - request not successful */
        DELETE_SUCCESS(0x08), /* Delete - request successful */
        DELETE_ERROR(0x09),    /* Delete - request successful */

        SERVER_STOPPED(0x0A),           /* Server is stopped, no requests are processed */
        SERVER_WRITE_LOCK(0x0B),        /* Server locked for out, only get possible */
        SERVER_NOT_RESPONSIBLE(0x0C)    /* Request not successful, server not responsible for key */
        ;



        byte code;


        Status(int code) {
            this.code = (byte) code;
        }

        public byte getCode() {
            return this.code;
        }

        public static Status getByCode(byte code){
            final Status[] all = Status.values();
            int i = code - all[0].getCode();
            if (i < 0 || i > all.length - 1 )
                return null;
            return all[i];
        }
    }




    /**
     * @return the key as String that is associated with this message,
     * null if not key is associated.
     */
    public String getKey();

    /**
     * @return the value as String that is associated with this message,
     * null if not value is associated.
     */
    public String getValue();

    /**
     * @return the key as byte array wrapped by type K that is associated with this message,
     * null if not key is associated.
     */
    public K getK();

    /**
     * @return the value as byte array wrapped by type V that is associated with this message,
     * null if not value is associated.
     */
    public V getV();

    /**
     * @return a status string that is used to identify request types,
     * response types and error types associated to the message.
     */
    public Status getStatus();
    
    /**
     * @return the metadata containing the hash ranges and addresses of each server
     */
    public Metadata getMetadata();

    public boolean isBatchData();

    public void setBatchData();

}
