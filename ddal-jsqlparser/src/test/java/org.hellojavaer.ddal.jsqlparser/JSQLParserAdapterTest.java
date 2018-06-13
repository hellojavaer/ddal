/*
 * #%L
 * ddal-jsqlparser
 * %%
 * Copyright (C) 2016 - 2018 the original author or authors.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, either version 2.1 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Lesser Public License for more details.
 *
 * You should have received a copy of the GNU General Lesser Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/lgpl-2.1.html>.
 * #L%
 */
package org.hellojavaer.ddal.jsqlparser;

import net.sf.jsqlparser.JSQLParserException;
import org.hellojavaer.ddal.core.utils.Assert;
import org.hellojavaer.ddal.ddr.datasource.exception.CrossPreparedStatementException;
import org.hellojavaer.ddal.ddr.shard.ShardRouter;
import org.hellojavaer.ddal.ddr.shard.simple.SimpleShardParser;
import org.hellojavaer.ddal.ddr.sqlparse.SQLParsedResult;
import org.hellojavaer.ddal.ddr.sqlparse.SQLParsedState;
import org.hellojavaer.ddal.ddr.sqlparse.exception.AmbiguousRouteResultException;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 2018/5/18.
 */
public class JSQLParserAdapterTest extends BaseTestShardParser {

    @Test
    public void testInitCheck() throws JSQLParserException {
        JSQLParserAdapter.checkJSqlParserFeature();
        JSQLParserAdapter.checkCompatibilityWithJSqlParser();
    }

    @Test
    public void testSelectLimitCheck00() {
        String sql = "select * from user where id = ?";
        ShardRouter shardRouter = buildParserForId().getShardRouter();
        JSQLParserAdapter jsqlParserAdapter = new JSQLParserAdapter(sql, shardRouter, false);
        SQLParsedState sqlParsedState = jsqlParserAdapter.parse();
        Map<Object, Object> jdbcParams = new HashMap<>();
        jdbcParams.put(1, 506);
        SQLParsedResult sqlParsedResult = sqlParsedState.parse(jdbcParams);
        Assert.equals(sqlParsedResult.getSql(), "SELECT * FROM db_02.user_0122 AS user WHERE id = ?");
        jsqlParserAdapter = new JSQLParserAdapter(sql, shardRouter, true);
        try {
            sqlParsedState = jsqlParserAdapter.parse();
            throw new Error();
        } catch (Exception e) {
        }
    }

    @Test
    public void testSelectLimitCheck01() {
        String sql = "select * from user where id = ? limit 10";
        ShardRouter shardRouter = buildParserForId().getShardRouter();
        JSQLParserAdapter jsqlParserAdapter = new JSQLParserAdapter(sql, shardRouter, false);
        SQLParsedState sqlParsedState = jsqlParserAdapter.parse();
        Map<Object, Object> jdbcParams = new HashMap<>();
        jdbcParams.put(1, 506);
        SQLParsedResult sqlParsedResult = sqlParsedState.parse(jdbcParams);
        Assert.equals(sqlParsedResult.getSql(), "SELECT * FROM db_02.user_0122 AS user WHERE id = ? LIMIT 10");

        jsqlParserAdapter = new JSQLParserAdapter(sql, shardRouter, true);
        sqlParsedState = jsqlParserAdapter.parse();
        jdbcParams = new HashMap<>();
        jdbcParams.put(1, 506);
        sqlParsedResult = sqlParsedState.parse(jdbcParams);
        Assert.equals(sqlParsedResult.getSql(), "SELECT * FROM db_02.user_0122 AS user WHERE id = ? LIMIT 10");
    }

    @Test
    public void testUpdateLimitCheck00() {
        String sql = "update user set name='abc' where id = ? limit 10";
        ShardRouter shardRouter = buildParserForId().getShardRouter();
        JSQLParserAdapter jsqlParserAdapter = new JSQLParserAdapter(sql, shardRouter, false);
        SQLParsedState sqlParsedState = jsqlParserAdapter.parse();
        Map<Object, Object> jdbcParams = new HashMap<>();
        jdbcParams.put(1, 506);
        SQLParsedResult sqlParsedResult = sqlParsedState.parse(jdbcParams);
        Assert.equals(sqlParsedResult.getSql(), "UPDATE db_02.user_0122 SET name = 'abc' WHERE id = ? LIMIT 10");

        jsqlParserAdapter = new JSQLParserAdapter(sql, shardRouter, true);
        sqlParsedState = jsqlParserAdapter.parse();
        jdbcParams = new HashMap<>();
        jdbcParams.put(1, 506);
        sqlParsedResult = sqlParsedState.parse(jdbcParams);
        Assert.equals(sqlParsedResult.getSql(), "UPDATE db_02.user_0122 SET name = 'abc' WHERE id = ? LIMIT 10");
    }

    @Test
    public void testDeleteLimitCheck00() {
        String sql = "delete from user where id = ? limit 10";
        ShardRouter shardRouter = buildParserForId().getShardRouter();
        JSQLParserAdapter jsqlParserAdapter = new JSQLParserAdapter(sql, shardRouter, false);
        SQLParsedState sqlParsedState = jsqlParserAdapter.parse();
        Map<Object, Object> jdbcParams = new HashMap<>();
        jdbcParams.put(1, 506);
        SQLParsedResult sqlParsedResult = sqlParsedState.parse(jdbcParams);
        Assert.equals(sqlParsedResult.getSql(), "DELETE FROM db_02.user_0122 WHERE id = ? LIMIT 10");

        jsqlParserAdapter = new JSQLParserAdapter(sql, shardRouter, true);
        sqlParsedState = jsqlParserAdapter.parse();
        jdbcParams = new HashMap<>();
        jdbcParams.put(1, 506);
        sqlParsedResult = sqlParsedState.parse(jdbcParams);
        Assert.equals(sqlParsedResult.getSql(), "DELETE FROM db_02.user_0122 WHERE id = ? LIMIT 10");
    }

    @Test
    public void testSqlParsedState00() {
        String sql = "select * from user where id = ?";
        SimpleShardParser parser = buildParserForId();
        JSQLParserAdapter jsqlParserAdapter = new JSQLParserAdapter(sql, parser.getShardRouter(), false);
        SQLParsedState sqlParsedState = jsqlParserAdapter.parse();
        //
        Map<Object, Object> jdbcParams = new HashMap<>();
        jdbcParams.put(1, 506);
        SQLParsedResult sqlParsedResult = sqlParsedState.parse(jdbcParams);
        Assert.equals(sqlParsedResult.getSql(), "SELECT * FROM db_02.user_0122 AS user WHERE id = ?");

        jdbcParams = new HashMap<>();
        jdbcParams.put(1, 507);
        sqlParsedResult = sqlParsedState.parse(jdbcParams);
        Assert.equals(sqlParsedResult.getSql(), "SELECT * FROM db_03.user_0123 AS user WHERE id = ?");
    }

    @Test
    public void testSqlParsedState01() {
        String sql = "select * from user where id = ?";
        SimpleShardParser parser = buildParserForId();
        JSQLParserAdapter jsqlParserAdapter = new JSQLParserAdapter(sql, parser.getShardRouter(), false);
        SQLParsedState sqlParsedState = jsqlParserAdapter.parse();
        //
        Map<Object, Object> jdbcParams = new HashMap<>();
        jdbcParams.put(1, 506);
        SQLParsedResult sqlParsedResult = sqlParsedState.parse(jdbcParams);
        Assert.equals(sqlParsedResult.getSql(), "SELECT * FROM db_02.user_0122 AS user WHERE id = ?");
        //
        jdbcParams = new HashMap<>();
        jdbcParams.put(1, 507);
        try {
            sqlParsedResult.checkIfCrossPreparedStatement(jdbcParams);
            throw new Error();
        } catch (CrossPreparedStatementException e) {

        }
    }

    @Test
    public void testSqlParsedState02() {
        String sql = "select * from user where id = ? and id = 506";
        SimpleShardParser parser = buildParserForId();
        JSQLParserAdapter jsqlParserAdapter = new JSQLParserAdapter(sql, parser.getShardRouter(), false);
        SQLParsedState sqlParsedState = jsqlParserAdapter.parse();
        //
        Map<Object, Object> jdbcParams = new HashMap<>();
        jdbcParams.put(1, 506);
        SQLParsedResult sqlParsedResult = sqlParsedState.parse(jdbcParams);
        Assert.equals(sqlParsedResult.getSql(), "SELECT * FROM db_02.user_0122 AS user WHERE id = ? AND id = 506");
        //
        jdbcParams = new HashMap<>();
        jdbcParams.put(1, 507);
        try {
            sqlParsedState.parse(jdbcParams);
            throw new Error();
        } catch (AmbiguousRouteResultException e) {
        }
    }
}
