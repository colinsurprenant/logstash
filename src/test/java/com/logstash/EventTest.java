package com.logstash;

import org.jruby.ir.operands.Hash;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class EventTest {

    @Test
    public void testBareToJson() throws Exception {
        Event e = new EventImpl();
        assertEquals("{\"@timestamp\":\"" + e.getTimestamp().toIso8601() + "\",\"@version\":\"1\"}", e.toJson());
    }

    @Test
    public void testSimpleFieldToJson() throws Exception {
        Map data = new HashMap();
        data.put("foo", "bar");
        Event e = new EventImpl(data);
        assertEquals("{\"@timestamp\":\"" + e.getTimestamp().toIso8601() + "\",\"foo\":\"bar\",\"@version\":\"1\"}", e.toJson());
    }
}