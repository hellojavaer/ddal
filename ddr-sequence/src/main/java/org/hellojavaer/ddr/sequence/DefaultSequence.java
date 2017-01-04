/*
 * Copyright 2016-2017 the original author or authors.
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
package org.hellojavaer.ddr.sequence;

/**
 * 步长
 * 阈值
 * 数据安全
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 03/01/2017.
 */
public class DefaultSequence implements Sequence {

    private static final int DEFAULT_CORE_SIZE = 10;

    private String           group;
    private String           tbName;
    private int              step;                                 // 单节点步长
    private int              timeout;
    private int              coreSize          = DEFAULT_CORE_SIZE; // 缓存队列大小
    private IdGetter         idGetter;

    private volatile IdCache idCache;

    public void init() {
        if (idCache == null) {
            synchronized (this) {
                if (idCache == null) {
                    idCache = getIdCache();
                }
            }
        }
    }

    protected IdCache getIdCache() {
        return new IdCache(coreSize <= 0 ? DEFAULT_CORE_SIZE : coreSize) {

            @Override
            public IdRange get() throws Exception {
                return getIdGetter().get(getGroup(), getTbName(), getStep());
            }
        };
    }

    @Override
    public long next() {
        init();
        try {
            return idCache.peek(timeout);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getTbName() {
        return tbName;
    }

    public void setTbName(String tbName) {
        this.tbName = tbName;
    }

    public int getStep() {
        return step;
    }

    public void setStep(int step) {
        this.step = step;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public int getCoreSize() {
        return coreSize;
    }

    public void setCoreSize(int coreSize) {
        this.coreSize = coreSize;
    }

    public IdGetter getIdGetter() {
        return idGetter;
    }

    public void setIdGetter(IdGetter idGetter) {
        this.idGetter = idGetter;
    }

}
