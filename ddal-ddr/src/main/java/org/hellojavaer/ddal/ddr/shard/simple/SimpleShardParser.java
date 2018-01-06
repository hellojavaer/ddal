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
package org.hellojavaer.ddal.ddr.shard.simple;

import org.hellojavaer.ddal.ddr.sqlparse.SQLParsedResult;
import org.hellojavaer.ddal.ddr.shard.ShardParser;
import org.hellojavaer.ddal.ddr.shard.ShardRouter;
import org.hellojavaer.ddal.ddr.sqlparse.SQLParsedState;
import org.hellojavaer.ddal.ddr.sqlparse.SQLParser;

import java.util.Map;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 15/11/2016.
 */
public class SimpleShardParser implements ShardParser {

    private ShardRouter shardRouter;
    private SQLParser   sqlParser;

    private SimpleShardParser() {
    }

    public SimpleShardParser(SQLParser sqlParser, ShardRouter shardRouter) {
        setSqlParser(sqlParser);
        setShardRouter(shardRouter);
    }

    public ShardRouter getShardRouter() {
        return shardRouter;
    }

    public void setShardRouter(ShardRouter shardRouter) {
        this.shardRouter = shardRouter;
    }

    public SQLParser getSqlParser() {
        return sqlParser;
    }

    public void setSqlParser(SQLParser sqlParser) {
        this.sqlParser = sqlParser;
    }

    @Override
    public SQLParsedResult parse(String sql, Map<Object, Object> jdbcParams) {
        SQLParsedState sqlParsedState = sqlParser.parse(sql, shardRouter);
        return sqlParsedState.parse(jdbcParams);
    }
}
