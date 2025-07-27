package cis5550.kvs;

import java.nio.charset.StandardCharsets;

public class RowData {
    String key;
    String colName;
    String value;
    String delimiter = "&";

    public RowData(String key, String colName, String value) {
        this.key = key;
        this.colName = colName;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public String getColName() {
        return colName;
    }

    public String getValue() {
        return value;
    }

    public byte[] toByteArray() {
        return (key + delimiter + colName + delimiter + value).getBytes(StandardCharsets.UTF_8);
    }
}
