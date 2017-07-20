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

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 20/07/2017.
 */
public class PollingGroupSequence implements GroupSequence {

    private final Sequence[]      sequences;

    private final ExecutorService executorService;

    private final AtomicInteger   count = new AtomicInteger(-1);

    static class InnerThreadFactory implements ThreadFactory {

        private static final AtomicInteger threadCount = new AtomicInteger(0);

        public Thread newThread(Runnable r) {
            return new Thread(r, "PollingGroupSequence-Thread-" + threadCount.getAndIncrement());
        }
    }

    public PollingGroupSequence(Collection<Sequence> sequences) {
        if (sequences == null || sequences.isEmpty()) {
            throw new IllegalArgumentException("sequences can't be empty");
        }
        this.sequences = new Sequence[sequences.size()];
        int i = 0;
        for (Sequence sequence : sequences) {
            this.sequences[i] = sequence;
            i++;
        }
        this.executorService = new ThreadPoolExecutor(0, this.sequences.length, 0L, TimeUnit.MILLISECONDS,
                                                      new LinkedBlockingQueue<Runnable>(), new InnerThreadFactory());
    }

    public PollingGroupSequence(Sequence... sequences) {
        if (sequences == null || sequences.length == 0) {
            throw new IllegalArgumentException("sequences can't be empty");
        }
        this.sequences = new Sequence[sequences.length];
        for (int i = 0; i < sequences.length; i++) {
            this.sequences[i] = sequences[i];
        }
        this.executorService = new ThreadPoolExecutor(0, this.sequences.length, 0L, TimeUnit.MILLISECONDS,
                                                      new LinkedBlockingQueue<Runnable>(), new InnerThreadFactory());
    }

    @Override
    public long nextValue(final long timeout, final TimeUnit timeUnit) throws GetSequenceTimeoutException {
        int start = count.incrementAndGet();
        if (start < 0) {
            synchronized (this) {
                start = count.get();
                if (start < 0) {
                    start = (Integer.MAX_VALUE % sequences.length + 1) % sequences.length;
                    count.set(start);
                } else {
                    start = count.incrementAndGet();
                }
            }
        }
        start = start % sequences.length;
        for (int i = start; i < sequences.length; i++) {
            try {
                return sequences[i].nextValue(0, TimeUnit.MILLISECONDS);
            } catch (GetSequenceTimeoutException e) {
                continue;
            }
        }
        for (int i = 0; i < start; i++) {
            try {
                return sequences[i].nextValue(0, TimeUnit.MILLISECONDS);
            } catch (GetSequenceTimeoutException e) {
                continue;
            }
        }
        synchronized (this) {
            for (int i = start; i < sequences.length; i++) {
                try {
                    return sequences[i].nextValue(0, TimeUnit.MILLISECONDS);
                } catch (GetSequenceTimeoutException e) {
                    continue;
                }
            }
            for (int i = 0; i < start; i++) {
                try {
                    return sequences[i].nextValue(0, TimeUnit.MILLISECONDS);
                } catch (GetSequenceTimeoutException e) {
                    continue;
                }
            }
            ExecutorCompletionService executorCompletionService = new ExecutorCompletionService(executorService);
            for (int i = 0; i < sequences.length; i++) {
                final Sequence sequence = sequences[i];
                executorCompletionService.submit(new Callable<Long>() {

                    @Override
                    public Long call() throws Exception {
                        return sequence.nextValue(timeout, timeUnit);
                    }
                });
            }
            try {
                // only get the first
                Future<Long> future = executorCompletionService.take();
                return future.get();
            } catch (InterruptedException e) {
                throw new SequenceException(e);
            } catch (ExecutionException e) {
                throw new SequenceException(e);
            } finally {
                // some ids may lost
                executorService.shutdownNow();
            }
        }
    }

}
