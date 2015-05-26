package com.logstash;

import java.util.Map;

public interface Event {

    String toString();

    void cancel();

    void uncancel();

    boolean isCancelled();

    Timestamp getTimestamp();

    void setTimestamp(Timestamp t);

    Object getField(String reference);

    void setField(String reference, Object value);

    boolean includes(String reference);

    Object remove(String reference);

    String toJson();

    // TBD see if we need that here or just as a to_hash in the JRuby layer
    Map toMap();

    Event overwrite(Event e);

    Event append(Event e);

    String sprintf(String s);

//    // TBD
//    Map to_hash_with_metadata();
//
//    // TBD
//    String to_json_with_metadata();
}
