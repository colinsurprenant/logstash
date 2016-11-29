package org.logstash.ackedqueue;

import org.logstash.common.io.CheckpointIO;
import org.logstash.common.io.PageIOFactory;

import java.util.List;

public class TestQueue extends Queue {
    public TestQueue(Settings settings) {
        super(settings);
    }

    public TestQueue(String dirPath, int pageCapacity, long maxBytes, CheckpointIO checkpointIO, PageIOFactory pageIOFactory, Class elementClass, int maxUnread, int checkpointMaxWrites, int checkpointMaxAcks, int checkpointMaxInterval) {
        super(dirPath, pageCapacity, maxBytes, checkpointIO, pageIOFactory, elementClass, maxUnread, checkpointMaxWrites, checkpointMaxAcks, checkpointMaxInterval);
    }

    public HeadPage getHeadPage() {
        return this.headPage;
    }

    public List<TailPage> getTailPages() {
        return this.tailPages;
    }
}
