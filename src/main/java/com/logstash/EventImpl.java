package com.logstash;

import jnr.posix.Times;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class EventImpl implements Event, Cloneable, Serializable {

    private boolean cancelled;
    private Map data;
    private Timestamp timestamp;
    private Accessors accessors;

    public EventImpl()
    {
        this.data = new HashMap<String, Object>();
        this.cancelled = false;
        this.timestamp = new Timestamp();
        this.accessors = new Accessors(this.data);
    }

    @Override
    public void cancel() {
        this.cancelled = true;
    }

    @Override
    public void uncancel() {
        this.cancelled = false;
    }

    @Override
    public boolean isCancelled() {
        return this.cancelled;
    }

    @Override
    public Timestamp getTimestamp() {
        return this.timestamp;
    }

    @Override
    public void setTimestamp(Timestamp t) {
        this.timestamp = t;
    }

    @Override
    public Object getField(String reference) {
        // TODO: add metadata support
        return this.accessors.get(reference);
    }

    @Override
    public void setField(String reference, Object value) {
        // TODO: add metadata support
        this.accessors.set(reference, value);
    }

    @Override
    public boolean includes(String reference) {
        // TODO: add metadata support
        return this.accessors.includes(reference);
    }

    @Override
    public String toJson() {
        return "";
    }

    @Override
    public Map toMap() {
        return this.data;
    }

    @Override
    public Event overwrite(Event e) {
        return null;
    }


    @Override
    public Event append(Event e) {
        return null;
    }

    @Override
    public Object remove(String path) {
        return this.accessors.del(path);
    }

    @Override
    public String sprintf(String s) {
        // TODO: implement sprintf
        return s;
    }

    public Event clone() {
        throw new UnsupportedOperationException("clone() not yet implemented");
    }

    public String toString() {
        return sprintf(getTimestamp().toIso8601() + " %{host} %{message}");
    }


//    @Override
//    public Map to_hash_with_metadata() {
//        return null;
//    }
//
//    @Override
//    public String to_json_with_metadata() {
//        return null;
//    }
}
