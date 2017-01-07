/*
 * Copyright 2017-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hellojavaer.ddal.sequence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 04/01/2017.
 */
public abstract class IdCache {

    private Logger                          logger         = LoggerFactory.getLogger(this.getClass());
    private LinkedBlockingQueue<InnerRange> list;

    private ReentrantLock                   writeLock      = new ReentrantLock();
    private Condition                       emptyCondition = writeLock.newCondition();
    private Condition                       fullCondition  = writeLock.newCondition();

    public IdCache(int capacity) {
        list = new LinkedBlockingQueue<>(capacity);
        startProducer();
    }

    // 数据库保持结束值
    protected class InnerRange {

        private long          beginValue;                        // 包含自身
        private long          endValue;                          // 包含自身
        private AtomicLong    count;                             //
        private AtomicBoolean deleted = new AtomicBoolean(false);

        public InnerRange(long beginValue, long endValue) {
            this.beginValue = beginValue;
            this.endValue = endValue;
            this.count = new AtomicLong(beginValue);
        }

        public long getBeginValue() {
            return beginValue;
        }

        public void setBeginValue(long beginValue) {
            this.beginValue = beginValue;
        }

        public long getEndValue() {
            return endValue;
        }

        public void setEndValue(long endValue) {
            this.endValue = endValue;
        }

        public AtomicLong getCount() {
            return count;
        }

        public void setCount(AtomicLong count) {
            this.count = count;
        }

        public AtomicBoolean getDeleted() {
            return deleted;
        }

        public void setDeleted(AtomicBoolean deleted) {
            this.deleted = deleted;
        }
    }

    private ThreadLocal<InnerRange> cache = new ThreadLocal<InnerRange>();

    public long peek(int timeout) throws InterruptedException, TimeoutException {
        InnerRange range = cache.get();
        if (range == null) {
            return peek0(timeout);
        } else {
            long i = range.getCount().getAndIncrement();
            if (i + 1 > range.getEndValue()) {
                cache.set(null);
                remove(range);
            }
            if (i <= range.getEndValue()) {
                return i;
            } else {
                return peek0(timeout);
            }
        }
    }

    private long peek0(int timeout) throws InterruptedException, TimeoutException {
        InnerRange range = list.peek();
        if (range == null) {
            writeLock.lock();
            try {
                int remainTime = timeout;
                while (true) {
                    long start = System.currentTimeMillis();
                    range = list.peek();
                    if (range == null) {
                        if (remainTime <= 0) {
                            throw new TimeoutException(Long.toString(timeout));
                        } else {
                            emptyCondition.await(timeout, TimeUnit.MILLISECONDS);
                            remainTime -= System.currentTimeMillis() - start;
                        }
                    } else {
                        break;
                    }
                }
            } finally {
                writeLock.unlock();
            }
        }
        long i = range.getCount().getAndIncrement();
        if (i + 1 > range.getEndValue()) {
            remove(range);
        } else {
            cache.set(range);
        }
        if (i <= range.getEndValue()) {
            return i;
        } else {
            return peek0(timeout);
        }
    }

    private void remove(InnerRange item) {
        writeLock.lock();
        try {
            if (item.getDeleted().compareAndSet(false, true)) {
                list.remove(item);
                fullCondition.signal();// one is enough
            }
        } finally {
            writeLock.unlock();
        }
    }

    private static AtomicInteger threadCount = new AtomicInteger(0);

    private void startProducer() {
        new Thread("IdCache-" + threadCount.getAndIncrement()) {

            @Override
            public void run() {
                final int baseLine = 5;
                final int[] sleepTimes = new int[] { 100, 200, 300, 500, 800, 1300, 2100, 3000 };
                final int endCount = sleepTimes.length + baseLine - 1;
                long count = 0;
                while (true) {
                    if (Thread.interrupted()) {
                        logger.error("[" + Thread.currentThread().getName() + " interrupted]");
                        break;
                    }
                    //
                    writeLock.lock();
                    try {
                        if (list.remainingCapacity() <= 0) {
                            fullCondition.await();
                        }
                        IdRange range = get();
                        list.put(new InnerRange(range.getBeginValue(), range.getEndValue()));
                        emptyCondition.signalAll();
                        count = 0;
                    } catch (Throwable e) {
                        logger.error("[GetIdRange]", e);
                        if (count >= baseLine) {
                            try {
                                Thread.sleep(count - baseLine);
                            } catch (InterruptedException e1) {
                                logger.error("[GetIdRangeSleep]", e1);
                            }
                        }
                        if (count < endCount) {
                            count++;
                        }
                    } finally {
                        writeLock.unlock();
                    }
                }
            }
        }.start();
    }

    public abstract IdRange get() throws Exception;
}
