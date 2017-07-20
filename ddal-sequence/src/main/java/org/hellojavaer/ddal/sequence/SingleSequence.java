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
import org.hellojavaer.ddal.sequence.exception.IllegalIdRangeException;
import org.hellojavaer.ddal.sequence.exception.NoAvailableIdRangeFoundException;
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

    private Logger           logger                        = LoggerFactory.getLogger(this.getClass());

    private static final int DEFAULT_DELAY_RETRY_BASE_LINE = 4;

    private String           schemaName;
    private String           tableName;
    private int              step;                                                                    // 单节点步长
    private int              cacheNSteps;                                                             // 缓存队列大小
    private int              initTimeout;
    private IdRangeGetter    idRangeGetter;
    private ExceptionHandler exceptionHandler;
    private int              delayRetryBaseLine;

    private IdCache          idCache;
    private boolean          initialized                   = false;

    public SingleSequence(String schemaName, String tableName, int step, int cacheNSteps, int initTimeout,
                          IdRangeGetter idRangeGetter) {
        this(schemaName, tableName, step, cacheNSteps, initTimeout, idRangeGetter, null, DEFAULT_DELAY_RETRY_BASE_LINE);
    }

    public SingleSequence(String schemaName, String tableName, int step, int cacheNSteps, int initTimeout,
                          IdRangeGetter idRangeGetter, ExceptionHandler exceptionHandler) {
        this(schemaName, tableName, step, cacheNSteps, initTimeout, idRangeGetter, exceptionHandler,
             DEFAULT_DELAY_RETRY_BASE_LINE);
    }

    public SingleSequence(String schemaName, String tableName, int step, int cacheNSteps, int initTimeout,
                          IdRangeGetter idRangeGetter, ExceptionHandler exceptionHandler, int delayRetryBaseLine) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.step = step;
        this.cacheNSteps = cacheNSteps;
        this.initTimeout = initTimeout;
        this.idRangeGetter = idRangeGetter;
        this.exceptionHandler = exceptionHandler;
        this.delayRetryBaseLine = delayRetryBaseLine;
    }

    public void init() {
        if (initialized == false) {
            synchronized (this) {
                if (initialized == false) {
                    Assert.notNull(schemaName, "'schemaName' can't be null'");
                    Assert.notNull(tableName, "'tableName' can't be null'");
                    Assert.isTrue(step > 0, "'step' must be greater than 0");
                    Assert.isTrue(cacheNSteps > 0, "'cacheNSteps' must be greater than 0");
                    Assert.isTrue(initTimeout > 0, "'initTimeout' must be greater than 0");
                    Assert.notNull(idRangeGetter, "'idRangeGetter' can't be null'");
                    Assert.isTrue(delayRetryBaseLine > 0, "'delayRetryBaseLine' must be greater than 0");
                    try {
                        this.idCache = new IdCache(step, cacheNSteps, initTimeout, exceptionHandler, delayRetryBaseLine) {

                            @Override
                            public IdRange getIdRange() throws Exception {
                                IdRange idRange = getIdRangeGetter().get(getSchemaName(), getTableName(), getStep());
                                if (idRange == null) {
                                    throw new NoAvailableIdRangeFoundException(
                                                                               "No available id rang was found for schemaName:'"
                                                                                       + getSchemaName()
                                                                                       + "', tableName:'"
                                                                                       + getTableName() + "'");
                                }
                                if (idRange.getBeginValue() > idRange.getEndValue()) {
                                    throw new IllegalIdRangeException("Illegal id range " + idRange
                                                                      + " for schemaName:'" + getSchemaName()
                                                                      + "', tableName:'" + getTableName() + "'");
                                }
                                return idRange;
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
            return idCache.get(timeout, timeUnit);
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

    public int getStep() {
        return step;
    }

    public void setStep(int step) {
        this.step = step;
    }

    public int getCacheNSteps() {
        return cacheNSteps;
    }

    public void setCacheNSteps(int cacheNSteps) {
        this.cacheNSteps = cacheNSteps;
    }

    public int getInitTimeout() {
        return initTimeout;
    }

    public void setInitTimeout(int initTimeout) {
        this.initTimeout = initTimeout;
    }

    public IdRangeGetter getIdRangeGetter() {
        return idRangeGetter;
    }

    public void setIdRangeGetter(IdRangeGetter idRangeGetter) {
        this.idRangeGetter = idRangeGetter;
    }

    public ExceptionHandler getExceptionHandler() {
        return exceptionHandler;
    }

    public void setExceptionHandler(ExceptionHandler exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
    }

    public int getDelayRetryBaseLine() {
        return delayRetryBaseLine;
    }

    public void setDelayRetryBaseLine(int delayRetryBaseLine) {
        this.delayRetryBaseLine = delayRetryBaseLine;
    }
}
