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

import org.hellojavaer.ddal.sequence.exception.DirtyDataException;
import org.hellojavaer.ddal.sequence.exception.NoAvailableIdRangeFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 04/01/2017.
 */
abstract class IdCache {

    private Logger           logger = LoggerFactory.getLogger(this.getClass());
    private SumBlockingQueue list;

    private int              step;
    private int              cacheNSteps;
    private ExceptionHandler exceptionHandler;

    public IdCache(int step, int cacheNSteps, ExceptionHandler exceptionHandler) {
        if (step <= 0) {
            throw new IllegalArgumentException("step must be greater than 0");
        }
        if (cacheNSteps <= 0) {
            throw new IllegalArgumentException("cacheNSteps must be greater than 0");
        }
        this.step = step;
        this.cacheNSteps = cacheNSteps;
        this.exceptionHandler = exceptionHandler;
        this.list = new SumBlockingQueue(step * cacheNSteps);
        startProducer();
    }

    public long get(int timeout) throws InterruptedException, TimeoutException {
        return list.get(timeout, TimeUnit.MILLISECONDS);
    }

    private static AtomicInteger threadCount = new AtomicInteger(0);

    private void startProducer() {
        new Thread("IdCache-" + threadCount.getAndIncrement()) {

            @Override
            public void run() {
                final int baseLine = 8;
                final int[] sleepTimes = new int[] { 100, 200, 300, 500, 800, 1300, 2100, 3000 };
                final int endCount = sleepTimes.length + baseLine - 1;
                long count = 0;
                while (true) {
                    if (Thread.interrupted()) {
                        logger.error("[" + Thread.currentThread().getName() + " interrupted]");
                        break;
                    }
                    //
                    try {
                        IdRange range = getIdRange();
                        int c = (int) ((range.getEndValue() - range.getBeginValue() + step) / step);
                        long beginValue = range.getBeginValue();
                        for (int i = 0; i < c; i++) {
                            long endValue = beginValue + step - 1;
                            endValue = endValue > range.getEndValue() ? range.getEndValue() : endValue;
                            list.put(new IdRange(beginValue, endValue));
                            beginValue += step;
                        }
                        count = 0;
                    } catch (Throwable e) {
                        if (exceptionHandler != null) {
                            if (exceptionHandler.handle(e)) {
                                continue;
                            }
                        }
                        if (e instanceof DirtyDataException) {
                            logger.error("[GetIdRange] " + e.getMessage());
                        } else if (e instanceof NoAvailableIdRangeFoundException) {
                            logger.error("[GetIdRange] " + e.getMessage());
                        } else {
                            logger.error("[GetIdRange]", e);
                        }
                        if (count >= baseLine) {
                            try {
                                Thread.sleep(sleepTimes[(int) (count - baseLine)]);
                            } catch (InterruptedException e1) {
                                logger.error("[GetIdRange] SleepException", e1);
                            }
                        }
                        if (count < endCount) {
                            count++;
                        }
                    }
                }
            }
        }.start();
    }

    public abstract IdRange getIdRange() throws Exception;
}
