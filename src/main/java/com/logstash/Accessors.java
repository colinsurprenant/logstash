package com.logstash;

import java.util.HashMap;
import java.util.Map;
import java.util.List;

public class Accessors {

    private Map<String, Object> data;
    protected Map<String, Object> lut;

    public Accessors(Map data) {
        this.data = data;
        this.lut = new HashMap(); // reference -> target LUT
    }

    public Object get(String reference) {
        FieldReference field = FieldReference.parse(reference);
        Object target = findTarget(field);
        return (target == null) ? null : fetch(target, field.getKey());
    }

    public Object set(String reference, Object value) {
        FieldReference field = FieldReference.parse(reference);
        Object target = findCreateTarget(field);
        return store(target, field.getKey(), value);
    }

    public Object del(String path) {
        // TODO: implement
        return null;
    }

    public boolean includes(String path) {
        // TODO: implemnent
        return false;
    }

    private Object findTarget(FieldReference field) {
        Object target;

        if ((target = this.lut.get(field.getReference())) != null) {
            return target;
        }

        target = this.data;
        for (String key : field.getPath()) {
            target = fetch(target, key);
            if (target == null) {
                return null;
            }
        }

        this.lut.put(field.getReference(), target);

        return target;
    }

    private Object findCreateTarget(FieldReference field) {
        Object target;

        if ((target = this.lut.get(field.getReference())) != null) {
            return target;
        }

        target = this.data;
        for (String key : field.getPath()) {
            Object result = fetch(target, key);
            if (result == null) {
                result = new HashMap<String, Object>();
                if (target instanceof Map) {
                    ((Map)target).put(key, result);
                } else if (target instanceof List) {
                    int i = Integer.parseInt(key);
                    // TODO: what about index out of bound?
                    ((List)target).set(i, result);
                } else {
                    throw new ClassCastException("expecting List or Map");
                }
            }
            target = result;
        }

        this.lut.put(field.getReference(), target);

        return target;
    }

    private Object fetch(Object target, String key) {
        if (target instanceof Map) {
            return ((Map) target).get(key);
        } else if (target instanceof List) {
            int i = Integer.parseInt(key);
            if (i < 0 || i >= ((List) target).size()) {
                return null;
            }
            return ((List) target).get(i);
        } else {
            throw new ClassCastException("expecting List or Map");
        }
    }

    private Object store(Object target, String key, Object value) {
        if (target instanceof Map) {
            ((Map) target).put(key, value);
        } else if (target instanceof List) {
            int i = Integer.parseInt(key);
            // TODO: what about index out of bound?
            ((List) target).set(i, value);
        } else {
            throw new ClassCastException("expecting List or Map");
        }
        return value;
    }
}
