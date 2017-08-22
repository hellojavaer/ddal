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

import org.hellojavaer.ddal.sequence.exception.GetSequenceTimeoutException;
import org.hellojavaer.ddal.sequence.exception.SequenceException;

import java.util.Collection;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 20/07/2017.
 */
public class PollingGroupSequence implements GroupSequence {

    private final ReentrantLock             lock = new ReentrantLock();

    private final LinkedCycleList<Sequence> cycleList;

    static class InnerThreadFactory implements ThreadFactory {

        private static final AtomicInteger threadCount = new AtomicInteger(0);

        public Thread newThread(Runnable r) {
            return new Thread(r, "PollingGroupSequence-Thread-" + threadCount.getAndIncrement());
        }
    }

    public PollingGroupSequence(Collection<? extends Sequence> sequences) {
        if (sequences == null || sequences.isEmpty()) {
            throw new IllegalArgumentException("sequences can't be empty");
        }
        this.cycleList = new LinkedCycleList(sequences);
    }

    public PollingGroupSequence(Sequence... sequences) {
        if (sequences == null || sequences.length == 0) {
            throw new IllegalArgumentException("sequences can't be empty");
        }
        this.cycleList = new LinkedCycleList(sequences);
    }

    @Override
    public long nextValue(long timeout, final TimeUnit timeUnit) throws GetSequenceTimeoutException {
        long now = System.nanoTime();
        cycleList.next();
        for (Sequence sequence : cycleList) {
            try {
                return sequence.nextValue(0, TimeUnit.NANOSECONDS);
            } catch (GetSequenceTimeoutException e) {
                continue;
            }
        }
        try {
            if (lock.tryLock(timeout, timeUnit) == false) {
                throw new GetSequenceTimeoutException(timeout + " " + timeUnit);
            }
        } catch (InterruptedException e) {
            throw new SequenceException(e);
        }
        try {
            final long nanTimeout = timeUnit.toNanos(timeout) - (System.nanoTime() - now);
            if (nanTimeout <= 0) {
                throw new GetSequenceTimeoutException(timeout + " " + timeUnit);
            }
            ExecutorService executorService = new ThreadPoolExecutor(this.cycleList.size(), this.cycleList.size(), 0L,
                                                                     TimeUnit.MILLISECONDS,
                                                                     new LinkedBlockingQueue<Runnable>(),
                                                                     new InnerThreadFactory());
            ExecutorCompletionService executorCompletionService = new ExecutorCompletionService(executorService);
            for (final Sequence sequence : cycleList) {
                executorCompletionService.submit(new Callable<Long>() {

                    @Override
                    public Long call() throws Exception {
                        return sequence.nextValue(nanTimeout, TimeUnit.NANOSECONDS);
                    }
                });
            }
            try {
                // only get the first
                Future<Long> future = executorCompletionService.take();
                // some ids may lost
                executorService.shutdownNow();
                return future.get();
            } catch (InterruptedException e) {
                throw new SequenceException(e);
            } catch (ExecutionException e) {
                throw new SequenceException(e);
            }
        } finally {
            lock.unlock();
        }
    }
}
