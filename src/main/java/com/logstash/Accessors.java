package com.logstash;

import java.util.Map;

public class Accessors {

    private Map store;

    public Accessors(Map store) {
        this.store = store;
    }

    public Object get(String path) {
        Path path = Path.parse(path);
        Object target;

        return null;
    }

    public void set(String path, Object value) {

    }

    public Object del(String path) {
        return null;
    }

    public boolean includes(String path) {
        return false;
    }
}
