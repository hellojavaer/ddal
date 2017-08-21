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

import org.hellojavaer.ddal.ddr.shard.simple.SimpleShardParser;
import org.junit.Test;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 22/06/2017.
 */
public class LimitTest extends BaseTestShardParser {

    @Test
    public void testInsert00() {
        SimpleShardParser shardParser = buildParserForId();
        shardParser.parse("insert into db.user00(id,name,`desc`) values(506,'allen','desc')", null);
        ((JSQLParser) shardParser.getSqlParser()).setEnableLimitCheck(true);
        shardParser.parse("insert into db.user00(id,name,`desc`) values(506,'allen','desc')", null);
    }

    @Test
    public void testDelete00() {
        SimpleShardParser shardParser = buildParserForId();
        shardParser.parse("delete from user00 limit 1", null);
        shardParser.parse("delete from user00 where id=1 limit 1", null);

        ((JSQLParser) shardParser.getSqlParser()).setEnableLimitCheck(true);
        shardParser.parse("delete from user00 limit 1", null);
        shardParser.parse("delete from user00 where id=1 limit 1", null);
        try {
            shardParser.parse("delete from user00", null);
            throw new Error();
        } catch (Exception e) {
        }
        try {
            shardParser.parse("delete from user00 where id=1", null);
            throw new Error();
        } catch (Exception e) {
        }
    }

    @Test
    public void testUpdate00() {
        SimpleShardParser shardParser = buildParserForId();
        shardParser.parse("update user00 set id = 1 limit 1", null);

        ((JSQLParser) shardParser.getSqlParser()).setEnableLimitCheck(true);
        shardParser.parse("update user00 set id = 1 limit 1", null);
        try {
            shardParser.parse("update user00 set id = 1", null);
            throw new Error();
        } catch (Exception e) {
        }
    }

    @Test
    public void testSelect00() {
        SimpleShardParser shardParser = buildParserForId();
        shardParser.parse("select * from user00 limit 1", null);
        shardParser.parse("select * from user00 where id = 1 limit 1", null);

        ((JSQLParser) shardParser.getSqlParser()).setEnableLimitCheck(true);
        shardParser.parse("select * from user00 limit 1", null);
        shardParser.parse("select * from user00 where id = 1 limit 1", null);
        try {
            shardParser.parse("select * from user00", null);
            throw new Error();
        } catch (Exception e) {
        }
        try {
            shardParser.parse("select * from user00 where id = 1", null);
            throw new Error();
        } catch (Exception e) {
        }
    }

}
