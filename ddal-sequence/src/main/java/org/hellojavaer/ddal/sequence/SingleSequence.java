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

    private Logger           logger      = LoggerFactory.getLogger(this.getClass());
    private String           schemaName;
    private String           tableName;
    private int              step;                                                  // 单节点步长
    private int              cacheNSteps;                                           // 缓存队列大小
    private IdGetter         idGetter;

    private volatile IdCache idCache;
    private boolean          initialized = false;

    private int              initTimeout;
    private int              getTimeout;
    private ExceptionHandler exceptionHandler;

    public SingleSequence(String schemaName, String tableName, int step, int cacheNSteps, int initTimeout,
                          IdGetter idGetter, int getTimeout) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.step = step;
        this.cacheNSteps = cacheNSteps;
        this.initTimeout = initTimeout;
        this.idGetter = idGetter;
        this.getTimeout = getTimeout;
        init();
    }

    public SingleSequence(String schemaName, String tableName, int step, int cacheNSteps, int initTimeout,
                          IdGetter idGetter, int getTimeout, ExceptionHandler exceptionHandler) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.step = step;
        this.cacheNSteps = cacheNSteps;
        this.initTimeout = initTimeout;
        this.idGetter = idGetter;
        this.getTimeout = getTimeout;
        this.exceptionHandler = exceptionHandler;
        init();
    }

    private void init() {
        if (initialized == false) {
            synchronized (this) {
                if (initialized == false) {
                    // init
                    Assert.notNull(schemaName, "'schemaName' can't be null'");
                    Assert.notNull(tableName, "'tableName' can't be null'");
                    Assert.notNull(step, "'step' must be greater than 0");
                    Assert.notNull(cacheNSteps, "'cacheNSteps' must be greater than or equal to 0");
                    Assert.notNull(initTimeout, "'initTimeout' must be greater than 0");
                    Assert.notNull(idGetter, "'idGetter' can't be null'");
                    Assert.notNull(getTimeout, "'getTimeout' must be greater than 0");
                    idCache = getIdCache();
                    initialized = true;
                }
            }
        }
    }

    protected IdCache getIdCache() {
        if (cacheNSteps <= 0) {
            throw new IllegalArgumentException("cacheNSteps[" + cacheNSteps + "] must greater then 0");
        }
        try {
            return new IdCache(step, cacheNSteps, initTimeout, exceptionHandler) {

                @Override
                public IdRange getIdRange() throws Exception {
                    IdRange idRange = getIdGetter().get(getSchemaName(), getTableName(), getStep());
                    if (idRange == null) {
                        throw new NoAvailableIdRangeFoundException("No available id rang was found for schemaName:'"
                                                                   + getSchemaName() + "', tableName:'"
                                                                   + getTableName() + "'");
                    } else {
                        return idRange;
                    }
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
            init();
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

    public IdGetter getIdGetter() {
        return idGetter;
    }

    public int getInitTimeout() {
        return initTimeout;
    }

    public int getGetTimeout() {
        return getTimeout;
    }

    public ExceptionHandler getExceptionHandler() {
        return exceptionHandler;
    }
}
