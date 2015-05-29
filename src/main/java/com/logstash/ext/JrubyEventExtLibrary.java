package com.logstash.ext;

import com.logstash.Event;
import com.logstash.EventImpl;
import com.logstash.Timestamp;
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

        public Event getEvent() {
            return event;
        }

        public void setEvent(Event event) {
            this.event = event;
        }

        // def initialize(data = {})
        @JRubyMethod(name = "initialize", optional = 1)
        public IRubyObject ruby_initialize(ThreadContext context, IRubyObject[] args)
        {
            args = Arity.scanArgs(context.runtime, args, 0, 1);
            IRubyObject data = args[0];

            if (data.isNil()) {
                this.event = new EventImpl();
            } else if (data instanceof Map) {
                this.event = new EventImpl((Map)data);
            } else if (Map.class.isAssignableFrom(data.getJavaClass())) {
                this.event = new EventImpl((Map)data.toJava(Map.class));
            } else {
                throw context.runtime.newTypeError("wrong argument type " + data.getMetaClass() + " (expected Hash)");
            }
            return context.nil;
        }

        @JRubyMethod(name = "[]", required = 1)
        public IRubyObject ruby_get_field(ThreadContext context, RubyString reference)
        {
            return JavaUtil.convertJavaToRuby(context.runtime, this.event.getField(reference.asJavaString()));
        }

        @JRubyMethod(name = "[]=", required = 2)
        public IRubyObject ruby_set_field(ThreadContext context, RubyString reference, IRubyObject value)
        {
            if (value instanceof RubyString) {
                this.event.setField(reference.asJavaString(), ((RubyString)value).asJavaString());
            } else if (value instanceof RubyInteger) {
                this.event.setField(reference.asJavaString(), ((RubyInteger)value).getLongValue());
            } else if (value instanceof RubyFloat) {
                this.event.setField(reference.asJavaString(), ((RubyFloat)value).getDoubleValue());
            }
            return value;
        }

        @JRubyMethod(name = "cancel")
        public IRubyObject ruby_cancel(ThreadContext context)
        {
            this.event.cancel();
            return RubyBoolean.createTrueClass(context.runtime);
        }

        @JRubyMethod(name = "uncancel")
        public IRubyObject ruby_uncancel(ThreadContext context)
        {
            this.event.uncancel();
            return RubyBoolean.createFalseClass(context.runtime);
        }

        @JRubyMethod(name = "cancelled?")
        public IRubyObject ruby_cancelled(ThreadContext context)
        {
            return RubyBoolean.newBoolean(context.runtime, this.event.isCancelled());
        }

        @JRubyMethod(name = "timestamp")
        public IRubyObject ruby_get_timestamp(ThreadContext context)
        {
            // TODO: properly implement
            return JavaUtil.convertJavaToRuby(context.runtime, this.event.getTimestamp());
        }

        @JRubyMethod(name = "timestamp=", required = 1)
        public IRubyObject ruby_set_timestamp(ThreadContext context, IRubyObject value)
        {
            // TODO: properly implement
            this.event.setTimestamp((Timestamp)value.toJava(Timestamp.class));
            return JavaUtil.convertJavaToRuby(context.runtime, this.event.getTimestamp());
        }

        @JRubyMethod(name = "include?", required = 1)
        public IRubyObject ruby_includes(ThreadContext context, RubyString reference)
        {
            return RubyBoolean.newBoolean(context.runtime, this.event.includes(reference.asJavaString()));
        }

        @JRubyMethod(name = "remove", required = 1)
        public IRubyObject ruby_remove(ThreadContext context, RubyString reference)
        {
            return JavaUtil.convertJavaToRuby(context.runtime, this.event.remove(reference.asJavaString()));
        }

        @JRubyMethod(name = "clone")
        public IRubyObject ruby_clone(ThreadContext context)
        {
            // TODO: no idea about getClassClass - wild guess - need to understasn how to properly instantiate a RubyEvent
            RubyEvent result = new RubyEvent(context.runtime, context.runtime.getClassClass());
            result.setEvent(this.event.clone());
            return result;
        }

        @JRubyMethod(name = "overwrite", required = 1)
        public IRubyObject ruby_overwrite(ThreadContext context, IRubyObject value)
        {
            // TODO: no idea about getClassClass - wild guess - need to understasn how to properly instantiate a RubyEvent
            RubyEvent result = new RubyEvent(context.runtime, context.runtime.getClassClass());
            result.setEvent(this.event.overwrite(((RubyEvent)value).event));
            return result;
        }

        @JRubyMethod(name = "append", required = 1)
        public IRubyObject ruby_append(ThreadContext context, IRubyObject value)
        {
            // TODO: no idea about getClassClass - wild guess - need to understasn how to properly instantiate a RubyEvent
            RubyEvent result = new RubyEvent(context.runtime, context.runtime.getClassClass());
            result.setEvent(this.event.append(((RubyEvent)value).event));
            return result;
        }

        @JRubyMethod(name = "sprintf", required = 1)
        public IRubyObject ruby_sprintf(ThreadContext context, IRubyObject format)
        {
            return RubyString.newString(context.runtime, event.sprintf(format.toString()));
        }

        @JRubyMethod(name = "to_s")
        public IRubyObject ruby_to_s(ThreadContext context)
        {
            return RubyString.newString(context.runtime, event.toString());
        }

        @JRubyMethod(name = "to_hash")
        public IRubyObject ruby_to_hash(ThreadContext context)
        {
            // TODO: should we explicitely convert to RubyHash?
            return JavaUtil.convertJavaToUsableRubyObject(context.runtime, this.event.toMap());
        }

        @JRubyMethod(name = "to_java")
        public IRubyObject ruby_to_java(ThreadContext context)
        {
            return JavaUtil.convertJavaToUsableRubyObject(context.runtime, this.event);
        }

        @JRubyMethod(name = "to_json", rest = true)
        public IRubyObject ruby_to_json(ThreadContext context, IRubyObject[] args)
            throws IOException
        {
            return RubyString.newString(context.runtime, event.toJson());
        }

        @JRubyMethod(name = "validate_value", required = 1, meta = true)
        public static IRubyObject ruby_validate_value(ThreadContext context, IRubyObject recv, IRubyObject value)
        {
            return value;
        }
    }
}
