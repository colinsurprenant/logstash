package com.logstash;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class PathTest {

    @Test
    public void testParseSingleBareField() throws Exception {
        Path path = Path.parse("foo");
        assertEquals(path.getPaths(), listify(new String[0]));
        assertEquals(path.getKey(), "foo");
    }

    @Test
    public void testParseSingleFieldPath() throws Exception {
        Path path = Path.parse("[foo]");
        assertEquals(path.getPaths(), listify(new String[0]));
        assertEquals(path.getKey(), "foo");
    }

    @Test
    public void testParse2FieldsPath() throws Exception {
        Path path = Path.parse("[foo][bar]");
        assertEquals(path.getPaths(), listify(new String[]{"foo"}));
        assertEquals(path.getKey(), "bar");
    }

    @Test
    public void testParse3FieldsPath() throws Exception {
        Path path = Path.parse("[foo][bar]]baz]");
        assertEquals(path.getPaths(), listify(new String[]{"foo", "bar"}));
        assertEquals(path.getKey(), "baz");
    }

    private List listify(String[] array) {
        return new ArrayList(Arrays.asList(array));
    }
}