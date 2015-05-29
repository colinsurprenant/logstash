package com.logstash;

import java.util.concurrent.ConcurrentHashMap;

public class PathCache {

    private static PathCache instance = null;
    private static ConcurrentHashMap<String, FieldReference> cache = new ConcurrentHashMap<>();

    protected PathCache() {}

    public static PathCache getInstance() {
        if (instance == null) {
            instance = new PathCache();
        }
        return instance;
    }

    FieldReference cache(String reference) {
        // atomicity between the get and put is not important
        FieldReference result = cache.get(reference);
        if (result == null) {
            result = FieldReference.parse(reference);
            cache.put(reference, result);
        }
        return result;
    }
}
