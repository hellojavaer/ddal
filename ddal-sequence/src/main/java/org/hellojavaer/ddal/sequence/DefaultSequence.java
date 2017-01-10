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

import org.hellojavaer.ddal.sequence.utils.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 步长
 * 阈值
 * 数据安全
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 03/01/2017.
 */
public class DefaultSequence implements Sequence {

    private Logger           logger      = LoggerFactory.getLogger(this.getClass());
    private String           groupName;
    private String           logicTableName;
    private Integer          step;                                                  // 单节点步长
    private Integer          cacheNSteps;                                           // 缓存队列大小
    private Integer          timeout;
    private IdGetter         idGetter;

    private volatile IdCache idCache;
    private boolean          initialized = false;

    public DefaultSequence() {
    }

    public DefaultSequence(String groupName, String logicTableName, Integer step, Integer cacheNSteps, Integer timeout,
                           IdGetter idGetter) {
        this.groupName = groupName;
        this.logicTableName = logicTableName;
        this.step = step;
        this.cacheNSteps = cacheNSteps;
        this.timeout = timeout;
        this.idGetter = idGetter;
        init();
    }

    public void init() {
        if (initialized == false) {
            synchronized (this) {
                if (initialized == false) {
                    // init
                    Assert.notNull(groupName, "'groupName' can't be null'");
                    Assert.notNull(logicTableName, "'logicTableName' can't be null'");
                    Assert.notNull(step, "'step' must be greater than 0");
                    Assert.notNull(cacheNSteps, "'cacheNSteps' must be greater than or equal to 0");
                    Assert.notNull(timeout, "'timeout' must be greater than 0");
                    Assert.notNull(idGetter, "'idGetter' can't be null'");
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
        return new IdCache(cacheNSteps) {

            @Override
            public IdRange get() throws Exception {
                IdRange idRange = getIdGetter().get(getGroupName(), getLogicTableName(), getStep());
                if (idRange == null) {
                    throw new NullPointerException("No id range is configured for groupName:'" + getGroupName()
                                                   + "', logicTableName:'" + getLogicTableName() + "'");
                } else {
                    return idRange;
                }
            }
        };
    }

    @Override
    public long nextValue() {
        init();
        try {
            return idCache.peek(timeout);
        } catch (RuntimeException e0) {
            throw e0;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public String getLogicTableName() {
        return logicTableName;
    }

    public void setLogicTableName(String logicTableName) {
        this.logicTableName = logicTableName;
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

    public Integer getTimeout() {
        return timeout;
    }

    public void setTimeout(Integer timeout) {
        this.timeout = timeout;
    }

    public IdGetter getIdGetter() {
        return idGetter;
    }

    public void setIdGetter(IdGetter idGetter) {
        this.idGetter = idGetter;
    }
}
