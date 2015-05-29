package com.logstash.ext;

import com.logstash.Event;
import com.logstash.EventImpl;
import org.jruby.*;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.javasupport.JavaUtil;
import org.jruby.runtime.Arity;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.runtime.load.Library;

import java.io.IOException;
import java.util.*;

public class JrubyEventExtLibrary implements Library {
    public void load(Ruby runtime, boolean wrap) throws IOException {
        RubyModule module = runtime.defineModule("LogStash");
        RubyClass clazz = runtime.defineClassUnder("Event", runtime.getObject(), new ObjectAllocator() {
            public IRubyObject allocate(Ruby runtime, RubyClass rubyClass) {
                return new RubyEvent(runtime, rubyClass);
            }
        }, module);
        clazz.defineAnnotatedMethods(RubyEvent.class);
    }

    @JRubyClass(name = "Event", parent = "Object")
    public static class RubyEvent extends RubyObject {

        private Event event;

        public RubyEvent(Ruby runtime, RubyClass klass) {
            super(runtime, klass);
        }

        // def initialize(data = {})
        @JRubyMethod(name = "initialize", optional = 1)
        public IRubyObject initialize(ThreadContext context, IRubyObject[] args)
        {
            args = Arity.scanArgs(context.runtime, args, 0, 1);
            IRubyObject data = args[0];

            if (data.isNil()) {
                this.event = new EventImpl();
            } else if (data instanceof Map) {
                this.event = new EventImpl((Map)data);
            } else {
                throw context.runtime.newTypeError("wrong argument type " + data.getMetaClass() + " (expected Hash)");
            }
            return context.nil;
        }

        // def to_java
        @JRubyMethod(name = "to_java")
        public IRubyObject toJava(ThreadContext context)
        {
            return JavaUtil.convertJavaToUsableRubyObject(context.runtime, this.event);
        }

        // def to_java
        @JRubyMethod(name = "to_json")
        public IRubyObject toJson(ThreadContext context)
            throws IOException
        {
            return RubyString.newString(context.runtime, event.toJson());
        }

        @JRubyMethod(name = "[]", required = 1)
        public IRubyObject getField(ThreadContext context, RubyString reference)
        {
            return JavaUtil.convertJavaToRuby(context.runtime, this.event.getField(reference.asJavaString()));
        }

        @JRubyMethod(name = "[]=", required = 2)
        public IRubyObject setField(ThreadContext context, RubyString reference, IRubyObject value)
        {
            // TODO: add value type guard here?
            this.event.setField(reference.asJavaString(), value);
            return value;
        }
    }
}
