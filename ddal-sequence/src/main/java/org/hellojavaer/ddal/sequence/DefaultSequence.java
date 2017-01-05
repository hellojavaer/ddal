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

/**
 * 步长
 * 阈值
 * 数据安全
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 03/01/2017.
 */
public class DefaultSequence implements Sequence {

    private String           group;
    private String           tbName;
    private int              step;       // 单节点步长
    private int              cacheNSteps; // 缓存队列大小
    private int              timeout;
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
        if (cacheNSteps <= 0) {
            throw new IllegalArgumentException("cacheNSteps[" + cacheNSteps + "] must greater then 0");
        }
        return new IdCache(cacheNSteps) {

            @Override
            public IdRange get() throws Exception {
                return getIdGetter().get(getGroup(), getTbName(), getStep());
            }
        };
    }

    @Override
    public long nextValue() {
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

    public int getCacheNSteps() {
        return cacheNSteps;
    }

    public void setCacheNSteps(int cacheNSteps) {
        this.cacheNSteps = cacheNSteps;
    }

    public IdGetter getIdGetter() {
        return idGetter;
    }

    public void setIdGetter(IdGetter idGetter) {
        this.idGetter = idGetter;
    }

}
