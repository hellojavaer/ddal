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
package org.hellojavaer.ddal.ddr.sqlparse.cache;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.Weighers;
import org.hellojavaer.ddal.ddr.shard.ShardRouter;
import org.hellojavaer.ddal.ddr.sqlparse.SQLParsedState;
import org.hellojavaer.ddal.ddr.sqlparse.SQLParser;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 11/01/2017.
 */
public class LRUSQLParseCache implements SQLParseCache {

    /** TODO key */
    private volatile ConcurrentLinkedHashMap<String, SQLParsedState> cache;
    private Integer                                                  capacity;
    private SQLParser                                                sqlParser;

    public LRUSQLParseCache() {
    }

    public LRUSQLParseCache(Integer capacity, SQLParser sqlParser) {
        this.capacity = capacity;
        this.sqlParser = sqlParser;
    }

    /**
     * 
     * 并发优先
     */
    @Override
    public SQLParsedState parse(String sql, ShardRouter shardRouter) {
        init();
        SQLParsedState result = cache.get(sql);
        if (result == null) {
            result = sqlParser.parse(sql, shardRouter);
            cache.put(sql, result);
        }
        return result;
    }

    private void init() {
        if (cache == null) {
            synchronized (this) {
                if (cache == null) {
                    cache = new ConcurrentLinkedHashMap.Builder<String, SQLParsedState>().maximumWeightedCapacity(capacity).weigher(Weighers.singleton()).build();
                }
            }
        }
    }

    public Integer getCapacity() {
        return capacity;
    }

    public void setCapacity(Integer capacity) {
        this.capacity = capacity;
    }

    public SQLParser getSqlParser() {
        return sqlParser;
    }

    public void setSqlParser(SQLParser sqlParser) {
        this.sqlParser = sqlParser;
    }
}
