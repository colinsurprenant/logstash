package com.logstash;

import java.util.List;
import java.util.ArrayList;

public class Accessors {

    public Accessors() {

    }

    public Object get(String path) {
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

    private Path parse(String path) {
        return Path.parse((path));
    }


//    def parse(accessor)
//    path = accessor.split(/[\[\]]/).select{|s| !s.empty?}
//    [path.pop, path]
//    end

}
