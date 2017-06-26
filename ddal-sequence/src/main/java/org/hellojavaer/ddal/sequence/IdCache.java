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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 04/01/2017.
 */
public abstract class IdCache {

    private Logger           logger         = LoggerFactory.getLogger(this.getClass());
    private SumBlockingQueue sumBlockingQueue;

    private int              step;
    private int              cacheNSteps;
    private int              initTimeout;
    private int              delayRetryBaseLine;
    private ExceptionHandler exceptionHandler;

    private AtomicBoolean    inited         = new AtomicBoolean(false);
    private CountDownLatch   countDownLatch = new CountDownLatch(1);

    public IdCache(int step, int cacheNSteps, int initTimeout, ExceptionHandler exceptionHandler, int delayRetryBaseLine)
                                                                                                                         throws InterruptedException,
                                                                                                                         TimeoutException {
        if (step <= 0) {
            throw new IllegalArgumentException("step must be greater than 0");
        }
        if (cacheNSteps <= 0) {
            throw new IllegalArgumentException("cacheNSteps must be greater than 0");
        }
        this.step = step;
        this.cacheNSteps = cacheNSteps;
        this.initTimeout = initTimeout;
        this.exceptionHandler = exceptionHandler;
        this.sumBlockingQueue = new SumBlockingQueue(step * cacheNSteps);
        this.delayRetryBaseLine = delayRetryBaseLine;
        //
        startProducer();
        //
        if (countDownLatch.await(initTimeout, TimeUnit.MILLISECONDS) == false) {
            throw new TimeoutException(initTimeout + " ms");
        }
    }

    public long get(int timeout) throws InterruptedException, TimeoutException {
        if (inited.get() == false) {
            if (countDownLatch.await(initTimeout, TimeUnit.MILLISECONDS) == false) {
                throw new TimeoutException(initTimeout + " ms");
            }
        }
        return sumBlockingQueue.get(timeout, TimeUnit.MILLISECONDS);
    }

    private static AtomicInteger threadCount = new AtomicInteger(0);

    private void startProducer() {
        new Thread(IdCache.class.getSimpleName() + "-" + threadCount.getAndIncrement()) {

            @Override
            public void run() {
                final int[] sleepTimes = new int[] { 100, 200, 300, 500, 800, 1300, 2100, 3000 };
                final int endCount = sleepTimes.length + delayRetryBaseLine - 1;
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
                            sumBlockingQueue.put(new IdRange(beginValue, endValue));
                            beginValue += step;
                            //
                            if (inited.get() == false && sumBlockingQueue.remainingSum() <= 0) {
                                inited.set(true);
                                countDownLatch.countDown();
                            }
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
                        if (count >= delayRetryBaseLine) {
                            try {
                                Thread.sleep(sleepTimes[(int) (count - delayRetryBaseLine)]);
                            } catch (InterruptedException e1) {
                                logger.error("[GetIdRange]", e1);
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
