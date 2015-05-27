package com.logstash;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class EventImpl implements Event, Cloneable, Serializable {

    private boolean cancelled;
    private Map<String, Object> data;
    private Timestamp timestamp;
    private Accessors accessors;

    private static final String TIMESTAMP = "@timestamp";
    private static final String TIMESTAMP_FAILURE_TAG = "_timestampparsefailure";
    private static final String TIMESTAMP_FAILURE_FIELD = "_@timestamp";
    private static final String VERSION = "@version";
    private static final String VERSION_ONE = "1";

    private static final ObjectMapper mapper = new ObjectMapper();

    // TODO: add metadata support

    public EventImpl()
    {
        this.data = new HashMap<String, Object>();
        this.data.put(VERSION, VERSION_ONE);
        this.cancelled = false;
        this.timestamp = new Timestamp();
        this.data.put(TIMESTAMP, this.timestamp);
        this.accessors = new Accessors(this.data);
    }

    public EventImpl(Map data)
    {
        this.data = data;
        this.data.putIfAbsent(VERSION, VERSION_ONE);
        this.cancelled = false;
        this.timestamp = initTimestamp(data.get(TIMESTAMP));
        this.data.put(TIMESTAMP, this.timestamp);
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
    public String toJson() throws IOException {
        return mapper.writeValueAsString(this.data);
    }

    @Override
    public Map toMap() {
        return this.data;
    }

    @Override
    public Event overwrite(Event e) {
        throw new UnsupportedOperationException("overwrite() not yet implemented");
    }


    @Override
    public Event append(Event e) {
        throw new UnsupportedOperationException("append() not yet implemented");
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
        // TODO: until we have sprintf
        String host = (String)this.data.getOrDefault("host", "%{host}");
        String message = (String)this.data.getOrDefault("message", "%{message}");
        return getTimestamp().toIso8601() + " " + host + " " + message;
    }

    private Timestamp initTimestamp(Object o) {
        try {
            if (o == null) {
                // most frequent
                return new Timestamp();
            } else if (o instanceof String) {
                // second most frequent
                return new Timestamp((String) o);
            } else if (o instanceof Timestamp) {
                return new Timestamp((Timestamp) o);
            } else if (o instanceof Long) {
                return new Timestamp((Long) o);
            } else {
                // TODO: add logging
                return new Timestamp();
            }
        } catch (IllegalArgumentException e) {
            // TODO: add error logging

            List<Object> tags = (List<Object>) this.data.get("tags");
            if (tags == null) {
                tags = new ArrayList<>();
                this.data.put("tags", tags);
            }

            if (!tags.contains(TIMESTAMP_FAILURE_TAG)) {
                tags.add(TIMESTAMP_FAILURE_TAG);
            }
            this.data.put(TIMESTAMP_FAILURE_FIELD, o.toString());

            return new Timestamp();
        }
    }
}
