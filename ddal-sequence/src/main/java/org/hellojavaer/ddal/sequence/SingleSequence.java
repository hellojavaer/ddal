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

import org.hellojavaer.ddal.core.utils.Assert;
import org.hellojavaer.ddal.sequence.exception.GetSequenceTimeoutException;
import org.hellojavaer.ddal.sequence.exception.IllegalSequenceRangeException;
import org.hellojavaer.ddal.sequence.exception.NoAvailableSequenceRangeFoundException;
import org.hellojavaer.ddal.sequence.exception.SequenceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 步长
 * 阈值
 * 数据安全
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 03/01/2017.
 */
public class SingleSequence implements Sequence {

    private Logger                 logger                        = LoggerFactory.getLogger(this.getClass());

    private static final int       DEFAULT_DELAY_RETRY_BASE_LINE = 4;

    private String                 schemaName;
    private String                 tableName;
    private Integer                step;                                                                    // 单节点步长
    private Integer                cacheNSteps;                                                             // 缓存队列大小
    private Integer                initTimeout;
    private SequenceRangeGetter    sequenceRangeGetter;
    private ExceptionHandler       exceptionHandler;
    private Integer                delayRetryBaseLine            = DEFAULT_DELAY_RETRY_BASE_LINE;

    private volatile SequenceCache sequenceCache;
    private boolean                initialized                   = false;

    public SingleSequence() {
    }

    public SingleSequence(String schemaName, String tableName, Integer step, Integer cacheNSteps, Integer initTimeout,
                          SequenceRangeGetter sequenceRangeGetter) {
        this(schemaName, tableName, step, cacheNSteps, initTimeout, sequenceRangeGetter, null,
             DEFAULT_DELAY_RETRY_BASE_LINE);
    }

    public SingleSequence(String schemaName, String tableName, Integer step, Integer cacheNSteps, Integer initTimeout,
                          SequenceRangeGetter sequenceRangeGetter, ExceptionHandler exceptionHandler) {
        this(schemaName, tableName, step, cacheNSteps, initTimeout, sequenceRangeGetter, exceptionHandler,
             DEFAULT_DELAY_RETRY_BASE_LINE);
    }

    public SingleSequence(String schemaName, String tableName, Integer step, Integer cacheNSteps, Integer initTimeout,
                          SequenceRangeGetter sequenceRangeGetter, ExceptionHandler exceptionHandler,
                          Integer delayRetryBaseLine) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.step = step;
        this.cacheNSteps = cacheNSteps;
        this.initTimeout = initTimeout;
        this.sequenceRangeGetter = sequenceRangeGetter;
        this.exceptionHandler = exceptionHandler;
        this.delayRetryBaseLine = delayRetryBaseLine;
        init();
    }

    public void init() {
        if (initialized == false) {
            synchronized (this) {
                if (initialized == false) {
                    Assert.notNull(schemaName, "'schemaName' can't be null'");
                    Assert.notNull(tableName, "'tableName' can't be null'");
                    Assert.notNull(step, "'step' can't be null'");
                    Assert.isTrue(step > 0, "'step' must be greater than 0");
                    Assert.notNull(cacheNSteps, "'cacheNSteps' can't be null'");
                    Assert.isTrue(cacheNSteps > 0, "'cacheNSteps' must be greater than 0");
                    Assert.notNull(initTimeout, "'initTimeout' can't be null'");
                    Assert.isTrue(initTimeout > 0, "'initTimeout' must be greater than 0");
                    Assert.notNull(sequenceRangeGetter, "'sequenceRangeGetter' can't be null'");
                    Assert.notNull(delayRetryBaseLine, "'delayRetryBaseLine' can't be null'");
                    Assert.isTrue(delayRetryBaseLine > 0, "'delayRetryBaseLine' must be greater than 0");
                    try {
                        this.sequenceCache = new SequenceCache(step, cacheNSteps, initTimeout, exceptionHandler,
                                                               delayRetryBaseLine) {

                            @Override
                            public SequenceRange getSequenceRange() throws Exception {
                                SequenceRange sequenceRange = getSequenceRangeGetter().get(getSchemaName(),
                                                                                           getTableName(), getStep());
                                if (sequenceRange == null) {
                                    throw new NoAvailableSequenceRangeFoundException(
                                                                                     "No available sequence rang was found for schemaName:'"
                                                                                             + getSchemaName()
                                                                                             + "', tableName:'"
                                                                                             + getTableName() + "'");
                                }
                                if (sequenceRange.getBeginValue() > sequenceRange.getEndValue()) {
                                    throw new IllegalSequenceRangeException("Illegal sequence range " + sequenceRange
                                                                            + " for schemaName:'" + getSchemaName()
                                                                            + "', tableName:'" + getTableName() + "'");
                                }
                                return sequenceRange;
                            }
                        };
                    } catch (InterruptedException e) {
                        throw new SequenceException(e);
                    } catch (TimeoutException e) {
                        throw new GetSequenceTimeoutException(e);
                    }
                    initialized = true;
                }
            }
        }
    }

    @Override
    public long nextValue(long timeout, TimeUnit timeUnit) throws GetSequenceTimeoutException {
        try {
            init();
            return sequenceCache.get(timeout, timeUnit);
        } catch (TimeoutException e1) {
            throw new GetSequenceTimeoutException(e1);
        } catch (SequenceException e0) {
            throw e0;
        } catch (Exception e) {
            throw new SequenceException(e);
        }
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Integer getStep() {
        return step;
    }

    public void setStep(Integer step) {
        this.step = step;
    }

    public Integer getCacheNSteps() {
        return cacheNSteps;
    }

    public void setCacheNSteps(Integer cacheNSteps) {
        this.cacheNSteps = cacheNSteps;
    }

    public Integer getInitTimeout() {
        return initTimeout;
    }

    public void setInitTimeout(Integer initTimeout) {
        this.initTimeout = initTimeout;
    }

    public SequenceRangeGetter getSequenceRangeGetter() {
        return sequenceRangeGetter;
    }

    public void setSequenceRangeGetter(SequenceRangeGetter sequenceRangeGetter) {
        this.sequenceRangeGetter = sequenceRangeGetter;
    }

    public ExceptionHandler getExceptionHandler() {
        return exceptionHandler;
    }

    public void setExceptionHandler(ExceptionHandler exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
    }

    public Integer getDelayRetryBaseLine() {
        return delayRetryBaseLine;
    }

    public void setDelayRetryBaseLine(Integer delayRetryBaseLine) {
        this.delayRetryBaseLine = delayRetryBaseLine;
    }
}
