// KeyValuePair class for handling nodes and their distances
class KeyValuePair implements Comparable<KeyValuePair> {
    private String key;
    private String value;
    private int distance;

    public KeyValuePair(String key, String value, int distance) {
        this.key = key;
        this.value = value;
        this.distance = distance;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public int getDistance() {
        return distance;
    }

    @Override
    public int compareTo(KeyValuePair other) {
        return Integer.compare(this.distance, other.distance);
    }

    @Override
    public String toString() { return key + "=" + value + " (dist=" + distance + ")"; }

}