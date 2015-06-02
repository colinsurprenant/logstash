package com.logstash;

/**
 * Created by ph on 15-05-22.
 */
public class EpochNode implements TemplateNode {
    public EpochNode(){ }

    @Override
    public String evaluate(Event event) {
        return String.valueOf(event.getTimestamp().getTime().getMillis() / 1000);
    }
}