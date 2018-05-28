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
public class InsertTest extends BaseTestShardParser {

    @Test
    public void test00() {
        ShardParser parser = buildParserForId();
        SQLParsedResult parsedResult = parser.parse("insert into db.user(id,name,`desc`) values(506,'allen','desc')",
                                                    null);
        Assert.equals(parsedResult.getSql(),
                      "INSERT INTO db_02.user_0122 (id, name, `desc`) VALUES (506, 'allen', 'desc')");
    }

    @Test
    public void test01() {
        ShardParser parser = buildParserForName();
        SQLParsedResult parsedResult = parser.parse("insert into db.user(id,name,`desc`) values(506,'allen','desc')",
                                                    null);
        Assert.equals(parsedResult.getSql(),
                      "INSERT INTO db_02.user_0010 (id, name, `desc`) VALUES (506, 'allen', 'desc')");
    }

    @Test
    public void test02() {
        ShardParser parser = buildParserForId();
        Map<Object, Object> map = new HashMap<Object, Object>();
        map.put(1, 506);
        map.put(2, "allen");
        map.put(3, "desc");
        SQLParsedResult parsedResult = parser.parse("insert into db.user(id,name,`desc`) values(?,?,?)", map);
        Assert.equals(parsedResult.getSql(), "INSERT INTO db_02.user_0122 (id, name, `desc`) VALUES (?, ?, ?)");
    }

    @Test
    public void test03() {
        ShardParser parser = buildParserForName();
        Map<Object, Object> map = new HashMap<Object, Object>();
        map.put(1, 506);
        map.put(2, "allen");
        map.put(3, "desc");
        SQLParsedResult parsedResult = parser.parse("insert into db.user(id,name,`desc`) values(?,?,?)", map);
        Assert.equals(parsedResult.getSql(), "INSERT INTO db_02.user_0010 (id, name, `desc`) VALUES (?, ?, ?)");
    }

    @Test
    public void test04() {
        ShardParser parser = buildParserForId();
        // 1
        try {
            parser.parse("INSERT INTO user(id,name ) VALUES( 506,'n1'), (507,'n2')", null);
            throw new Error();
        } catch (Exception e) {
        }

        // 2
        SQLParsedResult parsedResult = parser.parse("INSERT INTO user(id,name ) VALUES(506,'n1'), (634,'n2')", null);
        Assert.equals(parsedResult.getSql(), "INSERT INTO db_02.user_0122 (id, name) VALUES (506, 'n1'), (634, 'n2')");

        // 3
        Map<Object, Object> map = new HashMap<>();
        map.put(1, 507);
        try {
            parsedResult = parser.parse("INSERT INTO user(id,name ) VALUES(506,'n1'), (?,'n2')", map);
            throw new Error();
        } catch (Exception e) {
        }

        // 4
        map = new HashMap<>();
        map.put(1, 634);
        parsedResult = parser.parse("INSERT INTO user(id,name ) VALUES(506,'n1'), (?,'n2')", map);
        Assert.equals(parsedResult.getSql(), "INSERT INTO db_02.user_0122 (id, name) VALUES (506, 'n1'), (?, 'n2')");
    }

}
