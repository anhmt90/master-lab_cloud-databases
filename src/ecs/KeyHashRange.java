package ecs;

import util.HashUtils;
import util.Validate;

import java.io.Serializable;

/**
 * Structure handling Key ranges for the consistent hashing storage ring
 */
public class KeyHashRange implements Serializable {
    String start;
    String end;

    public KeyHashRange(String start, String end) {
        this.start = start;
        this.end = end;
        Validate.isTrue(isValidKeyRange(), "Invalid key range");
    }

    /**
     * Checks if a hex-String is within the hex range
     *
     * @param hashString hex-String that is checked
     * @return true if hex-String is within hash range
     */
    public boolean inRange(String hashString) {
        if(isWrappedAround()) {
            if(hashString.compareTo(start) > -1 && hashString.compareTo(HashUtils.MAX_HASH) < 1)
                return true;
            return hashString.compareTo(HashUtils.MIN_HASH) > -1 && hashString.compareTo(end) < 1;
        }
        return hashString.compareTo(start) > -1 && hashString.compareTo(end) < 1;
    }

    /**
     * Tests if this key range is contained within another range
     *
     * @param otherRange the other range
     * @return true if it is a subrange
     */
    public boolean isSubRangeOf(KeyHashRange otherRange) {
        boolean isStartGreaterEqual = HashUtils.compare(getStartBytes(), otherRange.getStartBytes()) >= 0;
        boolean isEndLessEqual = HashUtils.compare(getEndBytes(), otherRange.getEndBytes()) <= 0;

        boolean isSameState = !(this.isWrappedAround() ^ otherRange.isWrappedAround());
        if (isSameState)
            return isStartGreaterEqual && isEndLessEqual;
        else if (!this.isWrappedAround() && otherRange.isWrappedAround()) {
            if (isStartGreaterEqual && !isEndLessEqual)
                return true;
            else if (!isStartGreaterEqual && isEndLessEqual)
                return true;
        }
        return false;
    }

    public String getStart() {
        return start;
    }

    public String getEnd() {
        return end;
    }

    public byte[] getStartBytes() {
        return HashUtils.getHashBytesOf(start);
    }

    public byte[] getEndBytes() {
        return HashUtils.getHashBytesOf(end);
    }

    public boolean isWrappedAround() {
        return HashUtils.compare(getStartBytes(), getEndBytes()) > 0;
    }

    private boolean isValidKeyRange() {
        return start.length() == end.length() && !start.equals(end);
    }

    @Override
    public String toString() {
        return "KeyHashRange{" +
                "start='" + start + '\'' +
                ", end='" + end + '\'' +
                '}';
    }
}
