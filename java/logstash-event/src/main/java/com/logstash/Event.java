package com.logstash;

import com.logstash.ext.JrubyTimestampExtLibrary;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.jruby.RubyHash;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;


public class Event implements Cloneable, Serializable {

    private boolean cancelled;
    private Map<String, Object> data;
    private Map<String, Object> metadata;
    private Timestamp timestamp;
    private Accessors accessors;
    private Accessors metadata_accessors;

    private static final String METADATA = "@metadata";
    private static final String METADATA_BRACKETS = "[" + METADATA + "]";
    private static final String TIMESTAMP = "@timestamp";
    private static final String TIMESTAMP_FAILURE_TAG = "_timestampparsefailure";
    private static final String TIMESTAMP_FAILURE_FIELD = "_@timestamp";
    private static final String VERSION = "@version";
    private static final String VERSION_ONE = "1";

    private static final ObjectMapper mapper = new ObjectMapper();

    // TODO: add metadata support

    public Event()
    {
        this.metadata = new HashMap<String, Object>();
        this.data = new HashMap<String, Object>();
        this.data.put(VERSION, VERSION_ONE);
        this.cancelled = false;
        this.timestamp = new Timestamp();
        this.data.put(TIMESTAMP, this.timestamp);
        this.accessors = new Accessors(this.data);
        this.metadata_accessors = new Accessors(this.metadata);
    }

    public Event(Map data)
    {
        this.data = data;
        this.data.putIfAbsent(VERSION, VERSION_ONE);

        HashMap<String, Object> metadata = new HashMap();
        RubyHash rubyMetadataHash = (RubyHash) this.data.remove(METADATA);
        if (rubyMetadataHash != null) {
            Set<RubyHash.RubyHashEntry> entries = rubyMetadataHash.directEntrySet();
            for (RubyHash.RubyHashEntry e : entries) {
                metadata.put(e.getJavaifiedKey().toString(), e.getJavaifiedValue());
            }
        }

        this.metadata = metadata;
        this.metadata_accessors = new Accessors(metadata);

        this.cancelled = false;
        this.timestamp = initTimestamp(data.get(TIMESTAMP));
        this.data.put(TIMESTAMP, this.timestamp);
        this.accessors = new Accessors(this.data);
    }

    public Map<String, Object> getData() {
        return this.data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }

    public Accessors getAccessors() {
        return this.accessors;
    }

    public Accessors getMetadataAccessors() {
        return this.metadata_accessors;
    }

    public void setAccessors(Accessors accessors) {
        this.accessors = accessors;
    }

    public void setMetadataAccessors(Accessors accessors) {
        this.metadata_accessors = accessors;
    }

    public void cancel() {
        this.cancelled = true;
    }

    public void uncancel() {
        this.cancelled = false;
    }

    public boolean isCancelled() {
        return this.cancelled;
    }

    public Timestamp getTimestamp() throws IOException {
        if (this.data.containsKey(TIMESTAMP)) {
            return this.timestamp;
        } else {
            throw new IOException("fails");
        }
    }

    public void setTimestamp(Timestamp t) {
        this.timestamp = t;
        this.data.put(TIMESTAMP, this.timestamp);
    }

    public Object getField(String reference) {
        if (reference.startsWith(METADATA_BRACKETS)) {
            return this.metadata_accessors.get(reference.substring(METADATA_BRACKETS.length()));
        } else {
            return this.accessors.get(reference);
        }
    }

    public void setField(String reference, Object value) {
        if (reference.equals(TIMESTAMP)) {
            // TODO(talevy): check type of timestamp
            this.accessors.set(reference, value);
        } else if (reference.equals(METADATA_BRACKETS) || reference.equals(METADATA)) {
            this.metadata_accessors = new Accessors((RubyHash) value);
        } else if (reference.startsWith(METADATA_BRACKETS)) {
                this.metadata_accessors.set(reference.substring(METADATA_BRACKETS.length()), value);
        } else {
                this.accessors.set(reference, value);
        }
    }

    public boolean includes(String reference) {
        if (reference.equals(METADATA_BRACKETS) || reference.equals(METADATA)) {
            return true;
        } else if (reference.startsWith(METADATA_BRACKETS)) {
            return this.metadata_accessors.includes(reference.substring(METADATA_BRACKETS.length()));
        } else {
            return this.accessors.includes(reference);
        }
    }

    public String toJson() throws IOException {
        return mapper.writeValueAsString((Map<String, Object>)this.data);
    }

    public Map toMap() {
        return this.data;
    }

    public Event overwrite(Event e) {
        this.data = e.getData();
        this.accessors = e.getAccessors();
        this.cancelled = e.isCancelled();
        try {
            this.timestamp = e.getTimestamp();
        } catch (IOException exception) {
            this.timestamp = new Timestamp();
        }

        return this;
    }


    public Event append(Event e) {
        Util.mapMerge(this.data, e.data);

        return this;
    }

    public Object remove(String path) {
        return this.accessors.del(path);
    }

    public String sprintf(String s) throws IOException {
        return StringInterpolation.getInstance().evaluate(this, s);
    }

    public Event clone()
            throws CloneNotSupportedException
    {
        Event clone = (Event)super.clone();
        clone.setAccessors(new Accessors(clone.getData()));
        return clone;
    }

    public String toString() {
        // TODO: until we have sprintf
        String host = (String)this.data.getOrDefault("host", "%{host}");
        String message = (String)this.data.getOrDefault("message", "%{message}");
        try {
            return getTimestamp().toIso8601() + " " + host + " " + message;
        } catch (IOException e) {
            return host + " " + message;
        }
    }

    private Timestamp initTimestamp(Object o) {
        try {
            if (o == null) {
                // most frequent
                return new Timestamp();
            } else if (o instanceof String) {
                // second most frequent
                return new Timestamp((String) o);
            } else if (o instanceof JrubyTimestampExtLibrary.RubyTimestamp) {
                return new Timestamp(((JrubyTimestampExtLibrary.RubyTimestamp) o).getTimestamp());
            } else if (o instanceof Timestamp) {
                return new Timestamp((Timestamp) o);
            } else if (o instanceof Long) {
                return new Timestamp((Long) o);
            } else if (o instanceof DateTime) {
                return new Timestamp((DateTime) o);
            } else if (o instanceof Date) {
                return new Timestamp((Date) o);
            } else {
                // TODO: add logging
                return new Timestamp();
            }
        } catch (IllegalArgumentException e) {
            // TODO: add error logging
            tag(TIMESTAMP_FAILURE_TAG);
            this.data.put(TIMESTAMP_FAILURE_FIELD, o.toString());

            return new Timestamp();
        }
    }

    public void tag(String tag) {
        List<Object> tags = (List<Object>) this.data.get("tags");
        if (tags == null) {
            tags = new ArrayList<>();
            this.data.put("tags", tags);
        }

        if (!tags.contains(tag)) {
            tags.add(tag);
        }
    }
}
