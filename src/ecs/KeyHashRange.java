package ecs;

public class KeyHashRange {
	String start;
	String end;

	public KeyHashRange(String start, String end) {
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
		return false;
	}

}
