package com.logstash;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Path {

    private List<String> paths;
    private String key;
    private static List<String> EMPTY_STRINGS = new ArrayList(Arrays.asList(new String[]{""}));

    public Path(List<String> paths, String key) {
        this.paths = paths;
        this.key = key;
    }

    public List<String> getPaths() {
        return paths;
    }

    public String getKey() {
        return key;
    }

    public static Path parse(String path) {
        List<String> paths = new ArrayList(Arrays.asList(path.split("[\\[\\]]")));
        paths.removeAll(EMPTY_STRINGS);
        String key = paths.remove(paths.size() - 1);
        return new Path(paths, key);
    }
}
