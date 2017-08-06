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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 
 * This class is similar to java.util.concurrent.LinkedBlockingQueue.
 *
 * And this class is just for inner use.
 * 
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 06/025/2017.
 */
class SummedBlockingQueue {

    static class Node {

        InnerSequenceRange item;
        Node               next;

        Node(InnerSequenceRange x) {
            item = x;
        }
    }

    private final AtomicInteger                   countForCapacity = new AtomicInteger(0);
    private transient Node                        head;
    private transient Node                        last;
    private final ReentrantLock                   takeLock         = new ReentrantLock();
    private final Condition                       notEmpty         = takeLock.newCondition();
    private final ReentrantLock                   putLock          = new ReentrantLock();
    private final Condition                       notFull          = putLock.newCondition();

    private final long                            sum;
    private final AtomicLong                      countForSum      = new AtomicLong(0);

    private final ThreadLocal<InnerSequenceRange> threadLocal      = new ThreadLocal();

    public SummedBlockingQueue(long sum) {
        this.sum = sum;
        last = head = new Node(null);
    }

    static class InnerSequenceRange {

        private final long       beginValue;
        private final long       endValue;
        private final AtomicLong counter;

        public InnerSequenceRange(long beginValue, long endValue) {
            this.beginValue = beginValue;
            this.endValue = endValue;
            this.counter = new AtomicLong(beginValue);
        }

        public long getBeginValue() {
            return beginValue;
        }

        public long getEndValue() {
            return endValue;
        }

        public AtomicLong getCounter() {
            return counter;
        }
    }

    public void put(SequenceRange sequenceRange) throws InterruptedException {
        if (sequenceRange.getEndValue() < sequenceRange.getBeginValue()) {
            throw new IllegalArgumentException("end value must be greater than or equal to begin value");
        }
        Node node = new Node(new InnerSequenceRange(sequenceRange.getBeginValue(), sequenceRange.getEndValue()));
        final ReentrantLock putLock = this.putLock;
        final AtomicInteger count = this.countForCapacity;
        putLock.lockInterruptibly();
        long c = -1;
        try {
            while (countForSum.get() >= sum) {
                notFull.await();
            }
            enqueue(node);
            c = count.incrementAndGet();
            long s = countForSum.addAndGet(sequenceRange.getEndValue() - sequenceRange.getBeginValue() + 1);
            if (s < sum) {
                notFull.signal();
            }
        } finally {
            putLock.unlock();
        }
        if (c == 1) signalNotEmpty();
    }

    public long get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
        if (timeout < 0) {
            throw new IllegalArgumentException("'timeout' must be greater then or equal to 0");
        }
        if (unit == null) {
            throw new IllegalArgumentException("'unit' can't be null");
        }
        InnerSequenceRange range = threadLocal.get();
        if (range != null) {
            long id = range.getCounter().getAndIncrement();
            if (id <= range.getEndValue()) {
                long c = countForSum.decrementAndGet();
                if (c == sum - 1) {
                    signalNotFull();
                }
                if (id == range.getEndValue()) {
                    remove(range);
                    threadLocal.set(null);
                }
                return id;
            } else {
                remove(range);
                threadLocal.set(null);
                return recursiveGetFromQueue(timeout, unit);
            }
        } else {
            return recursiveGetFromQueue(timeout, unit);
        }
    }

    private long recursiveGetFromQueue(long timeout, TimeUnit unit) throws TimeoutException, InterruptedException {
        long nanoTimeout = unit.toNanos(timeout);
        while (true) {
            long now = System.nanoTime();
            InnerSequenceRange sequenceRange = get(nanoTimeout);
            if (sequenceRange == null) {
                throw new TimeoutException(timeout + " " + unit);
            } else {
                long id = sequenceRange.getCounter().getAndIncrement();
                if (id <= sequenceRange.getEndValue()) {
                    long c = countForSum.decrementAndGet();
                    if (c == sum - 1) {
                        signalNotFull();
                    }
                    if (id == sequenceRange.getEndValue()) {
                        remove(sequenceRange);
                    } else {
                        threadLocal.set(sequenceRange);
                    }
                    return id;
                } else {
                    remove(sequenceRange);
                    nanoTimeout -= System.nanoTime() - now;
                    if (nanoTimeout <= 0) {
                        throw new TimeoutException(timeout + " " + unit);
                    }
                }
            }
        }
    }

    private InnerSequenceRange get(long nanoTimeout) throws InterruptedException {
        final AtomicInteger count = this.countForCapacity;
        final ReentrantLock takeLock = this.takeLock;
        takeLock.lockInterruptibly();
        InnerSequenceRange x = null;
        try {
            while (count.get() == 0) {
                long now = System.nanoTime();
                if (nanoTimeout <= 0) {
                    return null;
                }
                if (notEmpty.awaitNanos(nanoTimeout) <= 0) {
                    return null;
                }
                nanoTimeout -= System.nanoTime() - now;
            }
            Node first = head.next;
            if (first == null) x = null;
            else x = first.item;

            if (count.get() > 0) {
                notEmpty.signal();
            }
        } finally {
            takeLock.unlock();
        }
        return x;
    }

    public boolean remove(Object o) {
        if (o == null) return false;
        fullyLock();
        try {
            for (Node trail = head, p = trail.next; p != null; trail = p, p = p.next) {
                if (o.equals(p.item)) {
                    unlink(p, trail);
                    return true;
                }
            }
            return false;
        } finally {
            fullyUnlock();
        }
    }

    void fullyLock() {
        putLock.lock();
        takeLock.lock();
    }

    void fullyUnlock() {
        takeLock.unlock();
        putLock.unlock();
    }

    void unlink(Node p, Node trail) {
        p.item = null;
        trail.next = p.next;
        if (last == p) last = trail;
        countForCapacity.getAndDecrement();
    }

    private void signalNotEmpty() {
        final ReentrantLock takeLock = this.takeLock;
        takeLock.lock();
        try {
            notEmpty.signal();
        } finally {
            takeLock.unlock();
        }
    }

    private void signalNotFull() {
        final ReentrantLock putLock = this.putLock;
        putLock.lock();
        try {
            notFull.signal();
        } finally {
            putLock.unlock();
        }
    }

    private void enqueue(Node node) {
        last = last.next = node;
    }

    public long remainingSum() {
        return sum - countForSum.get();
    }
}
