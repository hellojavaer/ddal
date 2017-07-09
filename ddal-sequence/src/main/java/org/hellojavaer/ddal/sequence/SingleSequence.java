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

import org.hellojavaer.ddal.sequence.exception.IllegalIdRangeException;
import org.hellojavaer.ddal.sequence.exception.NoAvailableIdRangeFoundException;
import org.hellojavaer.ddal.sequence.utils.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private IdGetter         idGetter;
    private int              getTimeout;
    private ExceptionHandler exceptionHandler;

    private IdCache          idCache;

    public SingleSequence(String schemaName, String tableName, int step, int cacheNSteps, int initTimeout,
                          IdGetter idGetter, int getTimeout) {
        this(schemaName, tableName, step, cacheNSteps, initTimeout, idGetter, getTimeout, null,
             DEFAULT_DELAY_RETRY_BASE_LINE);
    }

    public SingleSequence(String schemaName, String tableName, int step, int cacheNSteps, int initTimeout,
                          IdGetter idGetter, int getTimeout, ExceptionHandler exceptionHandler) {
        this(schemaName, tableName, step, cacheNSteps, initTimeout, idGetter, getTimeout, exceptionHandler,
             DEFAULT_DELAY_RETRY_BASE_LINE);
    }

    public SingleSequence(String schemaName, String tableName, int step, int cacheNSteps, int initTimeout,
                          IdGetter idGetter, int getTimeout, ExceptionHandler exceptionHandler, int delayRetryBaseLine) {
        Assert.notNull(schemaName, "'schemaName' can't be null'");
        Assert.notNull(tableName, "'tableName' can't be null'");
        Assert.isTrue(step > 0, "'step' must be greater than 0");
        Assert.isTrue(cacheNSteps > 0, "'cacheNSteps' must be greater than 0");
        Assert.isTrue(initTimeout > 0, "'initTimeout' must be greater than 0");
        Assert.notNull(idGetter, "'idGetter' can't be null'");
        Assert.isTrue(getTimeout > 0, "'getTimeout' must be greater than 0");
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.step = step;
        this.cacheNSteps = cacheNSteps;
        this.initTimeout = initTimeout;
        this.idGetter = idGetter;
        this.getTimeout = getTimeout;
        this.exceptionHandler = exceptionHandler;
        try {
            this.idCache = new IdCache(step, cacheNSteps, initTimeout, exceptionHandler, delayRetryBaseLine) {

                @Override
                public IdRange getIdRange() throws Exception {
                    IdRange idRange = getIdGetter().get(getSchemaName(), getTableName(), getStep());
                    if (idRange == null) {
                        throw new NoAvailableIdRangeFoundException("No available id rang was found for schemaName:'"
                                                                   + getSchemaName() + "', tableName:'"
                                                                   + getTableName() + "'");
                    }
                    if (idRange.getBeginValue() > idRange.getEndValue()) {
                        throw new IllegalIdRangeException("Illegal id range " + idRange + " for schemaName:'"
                                                          + getSchemaName() + "', tableName:'" + getTableName() + "'");
                    }
                    return idRange;
                }
            };
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long nextValue() {
        try {
            return idCache.get(getTimeout);
        } catch (RuntimeException e0) {
            throw e0;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public int getStep() {
        return step;
    }

    public int getCacheNSteps() {
        return cacheNSteps;
    }

    public int getInitTimeout() {
        return initTimeout;
    }

    public IdGetter getIdGetter() {
        return idGetter;
    }

    public int getGetTimeout() {
        return getTimeout;
    }

    public ExceptionHandler getExceptionHandler() {
        return exceptionHandler;
    }

    public IdCache getIdCache() {
        return idCache;
    }
}
