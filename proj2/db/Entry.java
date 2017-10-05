package db;

public class Entry<T> {
    private T value;
    private String type;

    public Entry(T newValue, String newType) {
        value = newValue;
        type = newType;
    }

    public String gettype() {
        return type;
    }

    public String floatstring() {
        String result = String.valueOf(value);
        String[] separated = result.split("\\.");

        int toChange = 3 - separated[1].length();

        while (toChange != 0) {
            if (toChange > 0) {
                result += "0";
                toChange--;
            } else {
                result = result.substring(0, result.length() - 1);
                toChange++;
            }
        }

        return result;
    }

    public T getvalue() {
        return value;
    }


}
