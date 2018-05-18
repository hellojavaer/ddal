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
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 18/12/2016.
 */
public class UpdateTest extends BaseTestShardParser {

    @Test
    public void test00() {
        ShardParser parser = buildParserForId();
        SQLParsedResult parsedResult = parser.parse("update db.user set `desc` = null where id = 11 and name = 'allen'",
                                                    null);
        Assert.equals(parsedResult.getSql(),
                      "UPDATE db_03.user_0011 SET `desc` = NULL WHERE id = 11 AND name = 'allen'");
    }

    @Test
    public void test01() {
        ShardParser parser = buildParserForName();
        SQLParsedResult parsedResult = parser.parse("update db.user set `desc` = null where id= 11 and name = 'allen'",
                                                    null);
        Assert.equals(parsedResult.getSql(),
                      "UPDATE db_02.user_0010 SET `desc` = NULL WHERE id = 11 AND name = 'allen'");
    }

    @Test
    public void test02() {
        ShardParser parser = buildParserForId();
        Map<Object, Object> map = new HashMap<Object, Object>();
        map.put(1, 506);
        map.put(2, "allen");
        map.put(3, "desc");
        SQLParsedResult parsedResult = parser.parse("update db.user set `desc` = null where id= ? and name = ?", map);
        Assert.equals(parsedResult.getSql(), "UPDATE db_02.user_0122 SET `desc` = NULL WHERE id = ? AND name = ?");
    }

    @Test
    public void test03() {
        ShardParser parser = buildParserForName();
        Map<Object, Object> map = new HashMap<Object, Object>();
        map.put(1, 11);
        map.put(2, "allen");
        map.put(3, "desc");
        SQLParsedResult parsedResult = parser.parse("update db.user set `desc` = null where id= ? and name = ?", map);
        Assert.equals(parsedResult.getSql(), "UPDATE db_02.user_0010 SET `desc` = NULL WHERE id = ? AND name = ?");
    }

    @Test
    public void test04() {
        ShardParser parser = buildParserForId();
        SQLParsedResult parsedResult = parser.parse("update db.user set `desc` = null where id= 506 and id in (select user_id from shop where user_id = 507)",
                                                    null);
        Assert.equals(parsedResult.getSql(),
                      "UPDATE db_02.user_0122 SET `desc` = NULL WHERE id = 506 AND id IN (SELECT user_id FROM db_03.shop_0123 AS shop WHERE user_id = 507)");
    }
}
