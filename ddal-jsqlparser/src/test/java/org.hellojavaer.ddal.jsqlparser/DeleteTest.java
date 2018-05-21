/*
 * #%L
 * ddal-jsqlparser
 * %%
 * Copyright (C) 2016 - 2017 the original author or authors.
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

import org.hellojavaer.ddal.core.utils.Assert;
import org.hellojavaer.ddal.ddr.shard.ShardParser;
import org.hellojavaer.ddal.ddr.sqlparse.SQLParsedResult;
import org.hellojavaer.ddal.ddr.sqlparse.exception.AmbiguousRouteResultException;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 18/12/2016.
 */
public class DeleteTest extends BaseTestShardParser {

    @Test
    public void test0() {
        ShardParser parser = buildParserForId();
        SQLParsedResult parsedResult = parser.parse("delete from db.user where user.id = 506 and name = " + "'allen'",
                                                    null);
        Assert.equals(parsedResult.getSql(), "DELETE FROM db_02.user_0122 WHERE user_0122.id = 506 AND name = 'allen'");
    }

    @Test
    public void test1() {
        ShardParser parser = buildParserForId();
        SQLParsedResult parsedResult = parser.parse("delete from db.user where db.user.id = 506 and name = "
                                                    + "'allen'", null);
        Assert.equals(parsedResult.getSql(), "DELETE FROM db_02.user_0122 WHERE db_02.user_0122.id = 506 AND "
                                             + "name = 'allen'");
    }

    @Test
    public void test2() {
        ShardParser parser = buildParserForId();
        try {
            SQLParsedResult parsedResult = parser.parse("delete from db.user where (db.user.id = 506 or db.user.id = 507) "
                                                                + "and name = " + "'allen'", null);
            throw new Error();
        } catch (AmbiguousRouteResultException e) {
            // ok
        }
    }

    @Test
    public void test00() {
        ShardParser parser = buildParserForId();
        SQLParsedResult parsedResult = parser.parse("delete from db.user AS user where id = 506 and name = 'allen'",
                                                    null);
        Assert.equals(parsedResult.getSql(), "DELETE FROM db_02.user_0122 AS user WHERE id = 506 AND name = 'allen'");
    }

    @Test
    public void test01() {
        ShardParser parser = buildParserForName();
        SQLParsedResult parsedResult = parser.parse("delete from db.user where id = 506 and name = 'allen'", null);
        Assert.equals(parsedResult.getSql(), "DELETE FROM db_02.user_0010 WHERE id = 506 AND name = 'allen'");
    }

    @Test
    public void test02() {
        ShardParser parser = buildParserForId();
        Map<Object, Object> map = new HashMap<Object, Object>();
        map.put(1, 506);
        map.put(2, "allen");
        map.put(3, "desc");
        SQLParsedResult parsedResult = parser.parse("DELETE FROM db.user WHERE id = ? AND name = ?", map);
        Assert.equals(parsedResult.getSql(), "DELETE FROM db_02.user_0122 WHERE id = ? AND name = ?");
    }

    @Test
    public void test03() {
        ShardParser parser = buildParserForName();
        Map<Object, Object> map = new HashMap<Object, Object>();
        map.put(1, 506);
        map.put(2, "allen");
        map.put(3, "desc");
        SQLParsedResult parsedResult = parser.parse("DELETE FROM db.user WHERE id = ? AND name = ?", map);
        Assert.equals(parsedResult.getSql(), "DELETE FROM db_02.user_0010 WHERE id = ? AND name = ?");
    }

    @Test
    public void test04() {
        ShardParser parser = buildParserForId();
        SQLParsedResult parsedResult = parser.parse("delete from db.user where id = 506 and id in (select user_id from shop where user_id = 507)",
                                                    null);
        Assert.equals(parsedResult.getSql(),
                      "DELETE FROM db_02.user_0122 WHERE id = 506 AND id IN (SELECT user_id FROM db_03.shop_0123 AS shop WHERE user_id = 507)");
    }

}
