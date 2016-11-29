package org.logstash.ackedqueue;

import org.jruby.RubyBoolean;
import org.logstash.common.io.ByteBufferPageIO;
import org.logstash.common.io.CheckpointIO;
import org.logstash.common.io.CheckpointIOFactory;
import org.logstash.common.io.MemoryCheckpointIO;
import org.logstash.common.io.PageIO;
import org.logstash.common.io.PageIOFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.both;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class QueueTest {

    @Test
    public void newQueue() throws IOException {
        Queue q = new TestQueue(TestSettings.getSettings(10));
        q.open();

        assertThat(q.nonBlockReadBatch(1), is(equalTo(null)));
    }

    @Test
    public void singleWriteRead() throws IOException {
        Queue q = new TestQueue(TestSettings.getSettings(100));
        q.open();

        Queueable element = new StringElement("foobarbaz");
        q.write(element);

        Batch b = q.nonBlockReadBatch(1);

        assertThat(b.getElements().size(), is(equalTo(1)));
        assertThat(b.getElements().get(0).toString(), is(equalTo(element.toString())));
        assertThat(q.nonBlockReadBatch(1), is(equalTo(null)));
    }

    @Test
    public void singleWriteMultiRead() throws IOException {
        Queue q = new TestQueue(TestSettings.getSettings(100));
        q.open();

        Queueable element = new StringElement("foobarbaz");
        q.write(element);

        Batch b = q.nonBlockReadBatch(2);

        assertThat(b.getElements().size(), is(equalTo(1)));
        assertThat(b.getElements().get(0).toString(), is(equalTo(element.toString())));
        assertThat(q.nonBlockReadBatch(2), is(equalTo(null)));
    }

    @Test
    public void multiWriteSamePage() throws IOException {
        Queue q = new TestQueue(TestSettings.getSettings(100));
        q.open();

        List<Queueable> elements = Arrays.asList(new StringElement("foobarbaz1"), new StringElement("foobarbaz2"), new StringElement("foobarbaz3"));

        for (Queueable e : elements) {
            q.write(e);
        }

        Batch b = q.nonBlockReadBatch(2);

        assertThat(b.getElements().size(), is(equalTo(2)));
        assertThat(b.getElements().get(0).toString(), is(equalTo(elements.get(0).toString())));
        assertThat(b.getElements().get(1).toString(), is(equalTo(elements.get(1).toString())));

        b = q.nonBlockReadBatch(2);

        assertThat(b.getElements().size(), is(equalTo(1)));
        assertThat(b.getElements().get(0).toString(), is(equalTo(elements.get(2).toString())));
    }

    @Test
    public void writeMultiPage() throws IOException {
        List<Queueable> elements = Arrays.asList(new StringElement("foobarbaz1"), new StringElement("foobarbaz2"), new StringElement("foobarbaz3"), new StringElement("foobarbaz4"));
        int singleElementCapacity = ByteBufferPageIO.HEADER_SIZE + ByteBufferPageIO._persistedByteCount(elements.get(0).serialize().length);

        TestQueue q = new TestQueue(TestSettings.getSettings(2 * singleElementCapacity));
        q.open();

        for (Queueable e : elements) {
            q.write(e);
        }

        // total of 2 pages: 1 head and 1 tail
        assertThat(q.getTailPages().size(), is(equalTo(1)));

        assertThat(q.getTailPages().get(0).isFullyRead(), is(equalTo(false)));
        assertThat(q.getTailPages().get(0).isFullyAcked(), is(equalTo(false)));
        assertThat(q.getHeadPage().isFullyRead(), is(equalTo(false)));
        assertThat(q.getHeadPage().isFullyAcked(), is(equalTo(false)));

        Batch b = q.nonBlockReadBatch(10);
        assertThat(b.getElements().size(), is(equalTo(2)));

        assertThat(q.getTailPages().size(), is(equalTo(1)));

        assertThat(q.getTailPages().get(0).isFullyRead(), is(equalTo(true)));
        assertThat(q.getTailPages().get(0).isFullyAcked(), is(equalTo(false)));
        assertThat(q.getHeadPage().isFullyRead(), is(equalTo(false)));
        assertThat(q.getHeadPage().isFullyAcked(), is(equalTo(false)));

        b = q.nonBlockReadBatch(10);
        assertThat(b.getElements().size(), is(equalTo(2)));

        assertThat(q.getTailPages().get(0).isFullyRead(), is(equalTo(true)));
        assertThat(q.getTailPages().get(0).isFullyAcked(), is(equalTo(false)));
        assertThat(q.getHeadPage().isFullyRead(), is(equalTo(true)));
        assertThat(q.getHeadPage().isFullyAcked(), is(equalTo(false)));

        b = q.nonBlockReadBatch(10);
        assertThat(b, is(equalTo(null)));
    }


    @Test
    public void writeMultiPageWithInOrderAcking() throws IOException {
        List<Queueable> elements = Arrays.asList(new StringElement("foobarbaz1"), new StringElement("foobarbaz2"), new StringElement("foobarbaz3"), new StringElement("foobarbaz4"));
        int singleElementCapacity = ByteBufferPageIO.HEADER_SIZE + ByteBufferPageIO._persistedByteCount(elements.get(0).serialize().length);

        TestQueue q = new TestQueue(TestSettings.getSettings(2 * singleElementCapacity));
        q.open();

        for (Queueable e : elements) {
            q.write(e);
        }

        Batch b = q.nonBlockReadBatch(10);

        assertThat(b.getElements().size(), is(equalTo(2)));
        assertThat(q.getTailPages().size(), is(equalTo(1)));

        // lets keep a ref to that tail page before acking
        TailPage tailPage = q.getTailPages().get(0);

        assertThat(tailPage.isFullyRead(), is(equalTo(true)));

        // ack first batch which includes all elements from tailPages
        b.close();

        assertThat(q.getTailPages().size(), is(equalTo(0)));
        assertThat(tailPage.isFullyRead(), is(equalTo(true)));
        assertThat(tailPage.isFullyAcked(), is(equalTo(true)));

        b = q.nonBlockReadBatch(10);

        assertThat(b.getElements().size(), is(equalTo(2)));
        assertThat(q.getHeadPage().isFullyRead(), is(equalTo(true)));
        assertThat(q.getHeadPage().isFullyAcked(), is(equalTo(false)));

        b.close();

        assertThat(q.getHeadPage().isFullyAcked(), is(equalTo(true)));
    }

    @Test
    public void writeMultiPageWithInOrderAckingCheckpoints() throws IOException {
        List<Queueable> elements1 = Arrays.asList(new StringElement("foobarbaz1"), new StringElement("foobarbaz2"));
        List<Queueable> elements2 = Arrays.asList(new StringElement("foobarbaz3"), new StringElement("foobarbaz4"));
        int singleElementCapacity = ByteBufferPageIO.HEADER_SIZE + ByteBufferPageIO._persistedByteCount(elements1.get(0).serialize().length);

        Settings settings = TestSettings.getSettings(2 * singleElementCapacity);
        settings.setCheckpointMaxWrites(1024); // arbritary high enough threshold so that it's not reached (default for TestSettings is 1)
        TestQueue q = new TestQueue(settings);
        q.open();

        assertThat(q.getHeadPage().getPageNum(), is(equalTo(0)));
        Checkpoint c = q.getCheckpointIO().read("checkpoint.head");
        assertThat(c.getPageNum(), is(equalTo(0)));
        assertThat(c.getElementCount(), is(equalTo(0)));
        assertThat(c.getMinSeqNum(), is(equalTo(0L)));
        assertThat(c.getFirstUnackedSeqNum(), is(equalTo(0L)));
        assertThat(c.getFirstUnackedPageNum(), is(equalTo(0)));

        for (Queueable e : elements1) {
            q.write(e);
        }

        c = q.getCheckpointIO().read("checkpoint.head");
        assertThat(c.getPageNum(), is(equalTo(0)));
        assertThat(c.getElementCount(), is(equalTo(0)));
        assertThat(c.getMinSeqNum(), is(equalTo(0L)));
        assertThat(c.getFirstUnackedSeqNum(), is(equalTo(0L)));
        assertThat(c.getFirstUnackedPageNum(), is(equalTo(0)));

//        assertThat(elements1.get(1).getSeqNum(), is(equalTo(2L)));
        q.ensurePersistedUpto(2);

        c = q.getCheckpointIO().read("checkpoint.head");
        assertThat(c.getPageNum(), is(equalTo(0)));
        assertThat(c.getElementCount(), is(equalTo(2)));
        assertThat(c.getMinSeqNum(), is(equalTo(1L)));
        assertThat(c.getFirstUnackedSeqNum(), is(equalTo(1L)));
        assertThat(c.getFirstUnackedPageNum(), is(equalTo(0)));

        for (Queueable e : elements2) {
            q.write(e);
        }

        c = q.getCheckpointIO().read("checkpoint.head");
        assertThat(c.getPageNum(), is(equalTo(1)));
        assertThat(c.getElementCount(), is(equalTo(0)));
        assertThat(c.getMinSeqNum(), is(equalTo(0L)));
        assertThat(c.getFirstUnackedSeqNum(), is(equalTo(0L)));
        assertThat(c.getFirstUnackedPageNum(), is(equalTo(0)));

        c = q.getCheckpointIO().read("checkpoint.0");
        assertThat(c.getPageNum(), is(equalTo(0)));
        assertThat(c.getElementCount(), is(equalTo(2)));
        assertThat(c.getMinSeqNum(), is(equalTo(1L)));
        assertThat(c.getFirstUnackedSeqNum(), is(equalTo(1L)));

        Batch b = q.nonBlockReadBatch(10);
        b.close();

        assertThat(q.getCheckpointIO().read("checkpoint.0"), is(nullValue()));

        c = q.getCheckpointIO().read("checkpoint.head");
        assertThat(c.getPageNum(), is(equalTo(1)));
        assertThat(c.getElementCount(), is(equalTo(2)));
        assertThat(c.getMinSeqNum(), is(equalTo(3L)));
        assertThat(c.getFirstUnackedSeqNum(), is(equalTo(3L)));
        assertThat(c.getFirstUnackedPageNum(), is(equalTo(1)));

        b = q.nonBlockReadBatch(10);
        b.close();

        c = q.getCheckpointIO().read("checkpoint.head");
        assertThat(c.getPageNum(), is(equalTo(1)));
        assertThat(c.getElementCount(), is(equalTo(2)));
        assertThat(c.getMinSeqNum(), is(equalTo(3L)));
        assertThat(c.getFirstUnackedSeqNum(), is(equalTo(5L)));
        assertThat(c.getFirstUnackedPageNum(), is(equalTo(1)));
    }

    @Test
    public void randomAcking() throws IOException {
        Random random = new Random();

        // 10 tests of random queue sizes
        for (int loop = 0; loop < 10; loop++) {
            int page_count = random.nextInt(10000) + 1;
            int digits = new Double(Math.ceil(Math.log10(page_count))).intValue();

            // create a queue with a single element per page
            List<Queueable> elements = new ArrayList<>();
            for (int i = 0; i < page_count; i++) {
                elements.add(new StringElement(String.format("%0" + digits + "d", i)));
            }
            int singleElementCapacity = ByteBufferPageIO.HEADER_SIZE + ByteBufferPageIO._persistedByteCount(elements.get(0).serialize().length);

            TestQueue q = new TestQueue(TestSettings.getSettings(singleElementCapacity));
            q.open();

            for (Queueable e : elements) {
                q.write(e);
            }

            assertThat(q.getTailPages().size(), is(equalTo(page_count - 1)));

            // first read all elements
            List<Batch> batches = new ArrayList<>();
            for (Batch b = q.nonBlockReadBatch(1); b != null; b = q.nonBlockReadBatch(1)) {
                batches.add(b);
            }
            assertThat(batches.size(), is(equalTo(page_count)));

            // then ack randomly
            Collections.shuffle(batches);
            for (Batch b : batches) {
                b.close();
            }

            assertThat(q.getTailPages().size(), is(equalTo(0)));
        }
    }

    @Test(timeout = 5000)
    public void reachMaxUnread() throws IOException, InterruptedException, ExecutionException {
        Queueable element = new StringElement("foobarbaz");
        int singleElementCapacity = ByteBufferPageIO.HEADER_SIZE + ByteBufferPageIO._persistedByteCount(element.serialize().length);

        Settings settings = TestSettings.getSettings(singleElementCapacity);
        settings.setMaxUnread(2); // 2 so we know the first write should not block and the second should
        TestQueue q = new TestQueue(settings);
        q.open();


        long seqNum = q.write(element);
        assertThat(seqNum, is(equalTo(1L)));
        assertThat(q.isFull(), is(false));

        int ELEMENT_COUNT = 1000;
        for (int i = 0; i < ELEMENT_COUNT; i++) {

            // we expect the next write call to block so let's wrap it in a Future
            Callable<Long> write = () -> {
                return q.write(element);
            };

            ExecutorService executor = Executors.newFixedThreadPool(1);
            Future<Long> future = executor.submit(write);

            while (!q.isFull()) {
                // spin wait until data is written and write blocks
                Thread.sleep(1);
            }
            assertThat(q.unreadCount, is(equalTo(2L)));
            assertThat(future.isDone(), is(false));

            // read one element, which will unblock the last write
            Batch b = q.nonBlockReadBatch(1);
            assertThat(b.getElements().size(), is(equalTo(1)));

            // future result is the blocked write seqNum for the second element
            assertThat(future.get(), is(equalTo(2L + i)));
            assertThat(q.isFull(), is(false));

            executor.shutdown();
        }

        // since we did not ack and pages hold a single item
        assertThat(q.getTailPages().size(), is(equalTo(ELEMENT_COUNT)));
    }

    @Test
    public void reachMaxUnreadWithAcking() throws IOException, InterruptedException, ExecutionException {
        Queueable element = new StringElement("foobarbaz");

        // TODO: add randomized testing on the page size (but must be > single element size)
        Settings settings = TestSettings.getSettings(256); // 256 is arbitrary, large enough to hold a few elements

        settings.setMaxUnread(2); // 2 so we know the first write should not block and the second should
        TestQueue q = new TestQueue(settings);
        q.open();

        // perform first non-blocking write
        long seqNum = q.write(element);

        assertThat(seqNum, is(equalTo(1L)));
        assertThat(q.isFull(), is(false));

        int ELEMENT_COUNT = 1000;
        for (int i = 0; i < ELEMENT_COUNT; i++) {

            // we expect this next write call to block so let's wrap it in a Future
            Callable<Long> write = () -> {
                return q.write(element);
            };

            ExecutorService executor = Executors.newFixedThreadPool(1);
            Future<Long> future = executor.submit(write);

            // spin wait until data is written and write blocks
            while (!q.isFull()) { Thread.sleep(1); }

            // read one element, which will unblock the last write
            Batch b = q.nonBlockReadBatch(1);
            assertThat(b, is(notNullValue()));
            assertThat(b.getElements().size(), is(equalTo(1)));
            b.close();

            // future result is the blocked write seqNum for the second element
            assertThat(future.get(), is(equalTo(2L + i)));
            assertThat(q.isFull(), is(false));

            executor.shutdown();
        }

        // all batches are acked, no tail pages should exist
        assertThat(q.getTailPages().size(), is(equalTo(0)));

        // the last read unblocked the last write so some elements (1 unread and maybe some acked) should be in the head page
        assertThat(q.getHeadPage().getElementCount() > 0L, is(true));
        assertThat(q.getHeadPage().unreadCount(), is(equalTo(1L)));
        assertThat(q.unreadCount, is(equalTo(1L)));
    }

    @Test(timeout = 5000)
    public void reachMaxSizeTest() throws IOException, InterruptedException, ExecutionException {
        Queueable element = new StringElement("0123456789"); // 10 bytes

        int singleElementCapacity = ByteBufferPageIO.HEADER_SIZE + ByteBufferPageIO._persistedByteCount(element.serialize().length);

        // allow 10 elements per page but only 100 events in total
        Settings settings = TestSettings.getSettings(singleElementCapacity * 10, singleElementCapacity * 100);

        TestQueue q = new TestQueue(settings);
        q.open();

        int ELEMENT_COUNT = 90; // should be able to write 99 events before getting full
        for (int i = 0; i < ELEMENT_COUNT; i++) {
            long seqNum = q.write(element);
        }

        assertThat(q.isFull(), is(false));

        // we expect this next write call to block so let's wrap it in a Future
        Callable<Long> write = () -> {
            return q.write(element);
        };

        ExecutorService executor = Executors.newFixedThreadPool(1);
        Future<Long> future = executor.submit(write);

        while (!q.isFull()) { Thread.sleep(10); }

        assertThat(q.isFull(), is(true));

        executor.shutdown();
    }

    @Test(timeout = 5000)
    public void resumeWriteOnNoLongerFullQueueTest() throws IOException, InterruptedException, ExecutionException {

        Queueable element = new StringElement("0123456789"); // 10 bytes

        int singleElementCapacity = ByteBufferPageIO.HEADER_SIZE + ByteBufferPageIO._persistedByteCount(element.serialize().length);

        // allow 10 elements per page but only 100 events in total
        Settings settings = TestSettings.getSettings(singleElementCapacity * 10, singleElementCapacity * 100);

        TestQueue q = new TestQueue(settings);
        q.open();

        int ELEMENT_COUNT = 90; // should be able to write 90 events (9 pages) before getting full
        for (int i = 0; i < ELEMENT_COUNT; i++) {
            long seqNum = q.write(element);
        }

        assertThat(q.isFull(), is(false));

        // we expect this next write call to block so let's wrap it in a Future
        Callable<Long> write = () -> {
            return q.write(element);
        };

        ExecutorService executor = Executors.newFixedThreadPool(1);
        Future<Long> future = executor.submit(write);

        while (!q.isFull()) { Thread.sleep(10); }

        assertThat(q.isFull(), is(true));

        Batch b = q.readBatch(10); // read 1 page (10 events)
        b.close();  // purge 1 page

        // spin wait until data is written and write blocks
        while (q.isFull()) { Thread.sleep(10); }

        assertThat(q.isFull(), is(false));

        executor.shutdown();
    }

    @Test(timeout = 5000)
    public void queueStillFullAfterPartialPageAckTest() throws IOException, InterruptedException, ExecutionException {

        Queueable element = new StringElement("0123456789"); // 10 bytes

        int singleElementCapacity = ByteBufferPageIO.HEADER_SIZE + ByteBufferPageIO._persistedByteCount(element.serialize().length);

        // allow 10 elements per page but only 100 events in total
        Settings settings = TestSettings.getSettings(singleElementCapacity * 10, singleElementCapacity * 100);

        TestQueue q = new TestQueue(settings);
        q.open();

        int ELEMENT_COUNT = 90; // should be able to write 99 events before getting full
        for (int i = 0; i < ELEMENT_COUNT; i++) {
            long seqNum = q.write(element);
        }

        assertThat(q.isFull(), is(false));

        // we expect this next write call to block so let's wrap it in a Future
        Callable<Long> write = () -> {
            return q.write(element);
        };

        ExecutorService executor = Executors.newFixedThreadPool(1);
        Future<Long> future = executor.submit(write);

        while (!q.isFull()) { Thread.sleep(10); }

        assertThat(q.isFull(), is(true));

        Batch b = q.readBatch(9); // read 90% of page (9 events)
        b.close();  // this should not purge a page

        assertThat(q.isFull(), is(true)); // queue should still be full

        executor.shutdown();
    }

    @Test
    public void recoverFreshQueue() throws IOException {
        MemoryCheckpointIO.clearSources();
        CheckpointIO cpIO = new MemoryCheckpointIO("foobar");

        // create pageIO facttory which just return an empty page and only expect page num 0
        PageIOFactory pageIOFactory = (pageNum, size, path) -> {
            assertThat(pageNum, is(equalTo(0)));
            return new ByteBufferPageIO(size);
        };

        // inject checkpoints
        Checkpoint cp = new Checkpoint(0, 0, 0, 0, 0);
        cpIO.write(cpIO.headFileName(), cp);

        TestQueue q = new TestQueue("foobbar", 1024, 0, cpIO, pageIOFactory, StringElement.class, 0, 1, 1, 0) ;
        q.open();

        assertThat(q.getTailPages().size(), is(equalTo(0)));
        assertThat(q.getHeadPage().getPageNum(), is(equalTo(0)));
        assertThat(q.getHeadPage().getMinSeqNum(), is(equalTo(0L)));
        assertThat(q.getHeadPage().getElementCount(), is(equalTo(0)));
        assertThat(q.getHeadPage().maxSeqNum(), is(equalTo(-1L)));
    }

    public final static int M_BYTES = 1024 * 1024;

    @Test
    public void recoverFullyAckedExistingQueue() throws IOException {
        MemoryCheckpointIO.clearSources();
        CheckpointIO cpIO = new MemoryCheckpointIO("foobar");

        PageIOFactory pageIOFactory = (pageNum, size, path) -> {
            assertThat(pageNum, is(both(greaterThanOrEqualTo(69)).and(lessThanOrEqualTo(70))));

            if (pageNum == 69) {
                // create a non-empty pageIO
                PageIO pageIO = new ByteBufferPageIO(M_BYTES);
                pageIO.create();
                for (long i = 0; i < 66; i++) {
                    pageIO.write("TEST".getBytes(), 600L + i);
                }
                return pageIO;
            } else {
                return new ByteBufferPageIO(size);
            }
        };

        // inject checkpoints
        Checkpoint cp = new Checkpoint(69, 69, 666, 600, 66);
        assertThat(cp.isFullyAcked(), is(true));

        cpIO.write(cpIO.headFileName(), cp);

        TestQueue q = new TestQueue("foobbar", M_BYTES, 0, cpIO, pageIOFactory, StringElement.class, 0, 1, 1, 0) ;
        q.open();

        // the headpage was non-empty buyt fully-acked so it was beheaded but the tail page adding
        // will discard it becaused it is fully acked so we should expect a new empty head page
        // and no tail pages at this point.

        assertThat(q.getTailPages().size(), is(equalTo(0)));
        assertThat(q.getHeadPage().getPageNum(), is(equalTo(70)));
        assertThat(q.getHeadPage().getMinSeqNum(), is(equalTo(0L)));
        assertThat(q.getHeadPage().maxSeqNum(), is(equalTo(-1L)));

        cp = cpIO.read(cpIO.headFileName());
        assertThat(cp.getPageNum(), is(equalTo(70)));
        assertThat(cp.getFirstUnackedPageNum(), is(equalTo(70)));
        assertThat(cp.getElementCount(), is(equalTo(0)));
        assertThat(cp.maxSeqNum(), is(equalTo(-1L)));
        assertThat(cp.getFirstUnackedSeqNum(), is(equalTo(0L)));
        assertThat(cp.getMinSeqNum(), is(equalTo(0L)));
    }

    @Test
    public void recoverNotFullyAckedExistingQueue() throws IOException {
        MemoryCheckpointIO.clearSources();
        CheckpointIO cpIO = new MemoryCheckpointIO("foobar");

        PageIOFactory pageIOFactory = (pageNum, size, path) -> {
            assertThat(pageNum, is(both(greaterThanOrEqualTo(69)).and(lessThanOrEqualTo(70))));

            if (pageNum == 69) {
                // create a non-empty pageIO
                PageIO pageIO = new ByteBufferPageIO(M_BYTES);
                pageIO.create();
                for (long i = 0; i < 66; i++) {
                    pageIO.write("TEST".getBytes(), 600L + i);
                }
                return pageIO;
            } else {
                return new ByteBufferPageIO(size);
            }
        };

        // inject checkpoints
        Checkpoint cp = new Checkpoint(69, 69, 660, 600, 66);
        assertThat(cp.isFullyAcked(), is(false));

        cpIO.write(cpIO.headFileName(), cp);

        TestQueue q = new TestQueue("foobbar", M_BYTES, 0, cpIO, pageIOFactory, StringElement.class, 0, 1, 1, 0) ;
        q.open();

        // the headpage was non-empty buyt fully-acked so it was beheaded but the tail page adding
        // will discard it becaused it is fully acked so we should expect a new empty head page
        // and no tail pages at this point.

        // tail page and checkpoint
        assertThat(q.getTailPages().size(), is(equalTo(1)));

        assertThat(q.getTailPages().get(0).getPageNum(), is(equalTo(69)));
        assertThat(q.getTailPages().get(0).getMinSeqNum(), is(equalTo(600L)));
        assertThat(q.getTailPages().get(0).maxSeqNum(), is(equalTo(665L)));
        assertThat(q.getTailPages().get(0).getElementCount(), is(equalTo(66)));
        assertThat(q.getTailPages().get(0).unreadCount(), is(equalTo(6L)));
        assertThat(q.getTailPages().get(0).firstUnackedSeqNum(), is(equalTo(660L)));
        assertThat(q.getTailPages().get(0).firstUnreadSeqNum, is(equalTo(660L)));
        assertThat(q.getTailPages().get(0).isFullyAcked(), is(false));
        assertThat(q.getTailPages().get(0).isFullyRead(), is(false));

        cp = cpIO.read(cpIO.tailFileName(69));
        assertThat(cp.getPageNum(), is(equalTo(69)));
        assertThat(cp.getFirstUnackedPageNum(), is(equalTo(0)));
        assertThat(cp.getElementCount(), is(equalTo(66)));
        assertThat(cp.maxSeqNum(), is(equalTo(665L)));
        assertThat(cp.getFirstUnackedSeqNum(), is(equalTo(660L)));
        assertThat(cp.getMinSeqNum(), is(equalTo(600L)));

        // head page and checkpoint
        assertThat(q.getHeadPage().getPageNum(), is(equalTo(70)));
        assertThat(q.getHeadPage().getMinSeqNum(), is(equalTo(0L)));
        assertThat(q.getHeadPage().maxSeqNum(), is(equalTo(-1L)));
        assertThat(q.getHeadPage().isFullyAcked(), is(false));
        assertThat(q.getHeadPage().isFullyRead(), is(true));

        cp = cpIO.read(cpIO.headFileName());
        assertThat(cp.getPageNum(), is(equalTo(70)));
        assertThat(cp.getFirstUnackedPageNum(), is(equalTo(69)));
        assertThat(cp.getElementCount(), is(equalTo(0)));
        assertThat(cp.maxSeqNum(), is(equalTo(-1L)));
        assertThat(cp.getFirstUnackedSeqNum(), is(equalTo(0L)));
        assertThat(cp.getMinSeqNum(), is(equalTo(0L)));
    }
}