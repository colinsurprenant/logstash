package com.logstash;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

public class Timestamp {

    private DateTime time;
    // TODO: is this DateTimeFormatter thread safe?
    private static DateTimeFormatter iso8601Formatter = ISODateTimeFormat.dateTime();

    public Timestamp() {
        this.time = new DateTime(DateTimeZone.UTC);
    }

    public Timestamp(String iso8601) {
        this.time = ISODateTimeFormat.dateTimeParser().parseDateTime(iso8601).toDateTime(DateTimeZone.UTC);
    }

    public Timestamp(Timestamp t) {
        this.time = t.getTime();
    }

    public Timestamp(long epoch_milliseconds) {
        this.time = new DateTime(epoch_milliseconds, DateTimeZone.UTC);
    }

    public DateTime getTime() {
        return time;
    }

    public static Timestamp now() {
        return new Timestamp();
    }

    public String toIso8601() {
        return this.iso8601Formatter.print(this.time);
    }

    public String toJson() {
        return "\"" + toIso8601() + "\"";
    }

    public String toString() {
        return toIso8601();
    }
}
