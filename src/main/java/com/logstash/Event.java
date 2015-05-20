package com.logstash;

import java.util.Map;

public interface Event {

    void cancel();

    void uncancel();

    boolean isCancelled();

    Timestamp getTimestamp();

    void setTimestamp(Timestamp t);

    Object getField(String path);

    void setField(String path, Object value);

    String toJson();

    // TBD see if we need that here or just as a to_hash in the JRuby layer
    Map toMap();

    Event overwrite(Event e);

    boolean includes(String path);

    Event append(Event e);

    Object remove(String path);

    String sprintf(String s);

//    // TBD
//    Map to_hash_with_metadata();
//
//    // TBD
//    String to_json_with_metadata();
}
