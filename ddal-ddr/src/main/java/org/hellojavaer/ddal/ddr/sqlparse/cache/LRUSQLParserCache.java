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
public class LRUSQLParserCache implements SQLParserCache {

    private volatile ConcurrentLinkedHashMap<InnerQueryKey, SQLParsedState> cache;
    private Integer                                                         capacity;
    private Integer                                                         maxSQLLength;
    private SQLParser                                                       sqlParser;

    private LRUSQLParserCache() {
    }

    public LRUSQLParserCache(SQLParser sqlParser, Integer capacity) {
        this(sqlParser, capacity, null);
    }

    public LRUSQLParserCache(SQLParser sqlParser, Integer capacity, Integer maxSQLLength) {
        this.sqlParser = sqlParser;
        this.capacity = capacity;
        this.maxSQLLength = maxSQLLength;
        init();
    }

    public Integer getCapacity() {
        return capacity;
    }

    public void setCapacity(Integer capacity) {
        this.capacity = capacity;
    }

    public Integer getMaxSQLLength() {
        return maxSQLLength;
    }

    public void setMaxSQLLength(Integer maxSQLLength) {
        this.maxSQLLength = maxSQLLength;
    }

    public SQLParser getSqlParser() {
        return sqlParser;
    }

    public void setSqlParser(SQLParser sqlParser) {
        this.sqlParser = sqlParser;
    }

    /**
     * 
     * 并发优先
     */
    @Override
    public SQLParsedState parse(String sql, ShardRouter shardRouter) {
        if (maxSQLLength != null && sql.length() > maxSQLLength) {
            return sqlParser.parse(sql, shardRouter);
        }// else
        init();
        InnerQueryKey queryKey = new InnerQueryKey(sql, shardRouter);
        SQLParsedState result = cache.get(queryKey);
        if (result == null) {
            result = sqlParser.parse(sql, shardRouter);
            cache.put(queryKey, result);
        }
        return result;
    }

    private void init() {
        if (cache == null) {
            synchronized (this) {
                if (cache == null) {
                    cache = new ConcurrentLinkedHashMap.Builder<InnerQueryKey, SQLParsedState>().maximumWeightedCapacity(capacity).weigher(Weighers.singleton()).build();
                }
            }
        }
    }

    private class InnerQueryKey {

        private String      sql;
        private ShardRouter shardRouter;

        public InnerQueryKey(String sql, ShardRouter shardRouter) {
            this.sql = sql;
            this.shardRouter = shardRouter;
        }

        @Override
        public int hashCode() {
            return sql.hashCode() + shardRouter.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj instanceof InnerQueryKey) {
                InnerQueryKey tar = (InnerQueryKey) obj;
                if ((sql == null && tar.getSql() == null || sql != null && sql.equals(tar.getSql())) && //
                    (shardRouter == null && tar.getShardRouter() == null || shardRouter != null
                                                                            && shardRouter.equals(tar.getShardRouter())) //
                ) {
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }

        public String getSql() {
            return sql;
        }

        public void setSql(String sql) {
            this.sql = sql;
        }

        public ShardRouter getShardRouter() {
            return shardRouter;
        }

        public void setShardRouter(ShardRouter shardRouter) {
            this.shardRouter = shardRouter;
        }
    }
}
