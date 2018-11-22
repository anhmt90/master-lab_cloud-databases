package ecs;

import util.HashUtils;
import util.Validate;

public class KeyHashRange {
    String start;
    String end;

    public KeyHashRange(String start, String end) {
        Validate.isTrue(isValidKeyRange(), "Invalid key range");
        this.start = start;
        this.end = end;
    }

    /**
     * Checks if a hex-String is within the hex range
     *
     * @param hexSample hex-String that is checked
     * @return true if hex-String is within hash range
     */
    public boolean inRange(String hexSample) {
        if (hexSample.compareTo(start) > -1 && hexSample.compareTo(end) == -1) {
            return true;
        }
        else if (isWrappedAround() && !(hexSample.compareTo(start) == -1 && hexSample.compareTo(end) > -1)) {
        	return true;
        }
        return false;
    }

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

    private boolean isWrappedAround() {
        return HashUtils.compare(getStartBytes(), getEndBytes()) > 0;
    }

    private boolean isValidKeyRange() {
        return start.length() == end.length() && !start.equals(end);
    }

}
