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
import org.hellojavaer.ddal.ddr.shard.ShardRouteContext;
import org.hellojavaer.ddal.ddr.shard.ShardRouteInfo;
import org.hellojavaer.ddal.ddr.shard.simple.SimpleShardParser;
import org.hellojavaer.ddal.ddr.sqlparse.SQLParsedResult;
import org.hellojavaer.ddal.ddr.sqlparse.exception.AmbiguousRouteResultException;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 18/12/2016.
 */
public class SelectTest extends BaseTestShardParser {

    @Before
    public void before() {
        ShardRouteContext.clearContext();
    }

    // ================ 别名测试 ==================
    @Test
    public void testAlias00() {
        ShardParser parser = buildParserForId();
        SQLParsedResult parsedResult = parser.parse("select * from user where id = 506", null);
        Assert.equals(parsedResult.getSql(), "SELECT * FROM db_02.user_0122 AS user WHERE id = 506");
        Set<String> expectedSchemas = new HashSet<>();
        expectedSchemas.add("db_02");
        Assert.isTrue(parsedResult.getSchemas().equals(expectedSchemas));
    }

    @Test
    public void testAlias01() {
        ShardParser parser = buildParserForId();
        SQLParsedResult parsedResult = parser.parse("select * from user user1 where id = 506", null);
        Assert.equals(parsedResult.getSql(), "SELECT * FROM db_02.user_0122 user1 WHERE id = 506");
        Set<String> expectedSchemas = new HashSet<>();
        expectedSchemas.add("db_02");
        Assert.isTrue(parsedResult.getSchemas().equals(expectedSchemas));
    }

    @Test
    public void testAlias02() {
        ShardParser parser = buildParserForId();
        SQLParsedResult parsedResult = parser.parse("select * from user user1 where user1.id = 506", null);
        Assert.equals(parsedResult.getSql(), "SELECT * FROM db_02.user_0122 user1 WHERE user1.id = 506");
        Set<String> expectedSchemas = new HashSet<>();
        expectedSchemas.add("db_02");
        Assert.isTrue(parsedResult.getSchemas().equals(expectedSchemas));
    }

    @Test
    public void testAlias03() {
        ShardParser parser = buildParserForId();
        SQLParsedResult parsedResult = parser.parse("select * from db.user user0, db.user user1 where user0.id = 506 and user1.id = 507 ",
                                                    null);
        Assert.equals(parsedResult.getSql(),
                      "SELECT * FROM db_02.user_0122 user0, db_03.user_0123 user1 WHERE user0.id = 506 AND user1.id = 507");
        Set<String> expectedSchemas = new HashSet<>();
        expectedSchemas.add("db_02");
        expectedSchemas.add("db_03");
        Assert.isTrue(parsedResult.getSchemas().equals(expectedSchemas));
    }

    // ==================== 大小写测试 ==================
    @Test
    public void testLetter00() {
        ShardParser parser = buildParserForId();
        SQLParsedResult parsedResult = parser.parse("select * from useR where usEr.iD = 506", null);
        Assert.equals(parsedResult.getSql(), "SELECT * FROM db_02.user_0122 AS useR WHERE usEr.iD = 506");
        Set<String> expectedSchemas = new HashSet<>();
        expectedSchemas.add("db_02");
        Assert.isTrue(parsedResult.getSchemas().equals(expectedSchemas));
    }

    @Test
    public void testLetter01() {
        ShardParser parser = buildParserForId();
        SQLParsedResult parsedResult = parser.parse("select * from useR useR1 where usEr1.iD = 506", null);
        Assert.equals(parsedResult.getSql(), "SELECT * FROM db_02.user_0122 useR1 WHERE usEr1.iD = 506");
        Set<String> expectedSchemas = new HashSet<>();
        expectedSchemas.add("db_02");
        Assert.isTrue(parsedResult.getSchemas().equals(expectedSchemas));
    }

    // =============== 分片列命中查询 ================
    @Test
    public void testHitSdKeyWithEqual00() {
        ShardParser parser = buildParserForId();
        SQLParsedResult parsedResult = parser.parse("select * from db.user where id = 506", null);
        Assert.equals(parsedResult.getSql(), "SELECT * FROM db_02.user_0122 AS user WHERE id = 506");
        Set<String> expectedSchemas = new HashSet<>();
        expectedSchemas.add("db_02");
        Assert.isTrue(parsedResult.getSchemas().equals(expectedSchemas));
    }

    @Test
    public void testHitSdKeyWithEqual01() {
        ShardParser parser = buildParserForId();
        Map<Object, Object> map = new HashMap<>();
        map.put(1, 506);
        SQLParsedResult parsedResult = parser.parse("select * from db.user where id = ?", map);
        Assert.equals(parsedResult.getSql(), "SELECT * FROM db_02.user_0122 AS user WHERE id = ?");
        Set<String> expectedSchemas = new HashSet<>();
        expectedSchemas.add("db_02");
        Assert.isTrue(parsedResult.getSchemas().equals(expectedSchemas));

        map.put(1, 507);
        parsedResult = parser.parse("select * from db.user where id = ?", map);
        Assert.equals(parsedResult.getSql(), "SELECT * FROM db_03.user_0123 AS user WHERE id = ?");
        expectedSchemas = new HashSet<>();
        expectedSchemas.add("db_03");
        Assert.isTrue(parsedResult.getSchemas().equals(expectedSchemas));
    }

    @Test
    public void testHitSdKeyWithIn00() {
        ShardParser parser = buildParserForId();
        SQLParsedResult parsedResult = parser.parse("select * from db.user where id in (506,634,762)", null);
        Assert.equals(parsedResult.getSql(), "SELECT * FROM db_02.user_0122 AS user WHERE id IN (506, 634, 762)");
        Set<String> expectedSchemas = new HashSet<>();
        expectedSchemas.add("db_02");
        Assert.isTrue(parsedResult.getSchemas().equals(expectedSchemas));
    }

    @Test
    public void testHitSdKeyWithIn01() {
        ShardParser parser = buildParserForId();
        Map<Object, Object> map = new HashMap<>();
        map.put(1, 506);
        map.put(2, 634);
        map.put(3, 762);
        SQLParsedResult parsedResult = parser.parse("select * from db.user where id in (?,?,?)", map);
        Assert.equals(parsedResult.getSql(), "SELECT * FROM db_02.user_0122 AS user WHERE id IN (?, ?, ?)");
        Set<String> expectedSchemas = new HashSet<>();
        expectedSchemas.add("db_02");
        Assert.isTrue(parsedResult.getSchemas().equals(expectedSchemas));
    }

    // between 和 in 允许混合jdbc参数和sql参数
    @Test
    public void testHitSdKeyWithIn02() {
        ShardParser parser = buildParserForId();
        Map<Object, Object> map = new HashMap<>();
        map.put(1, 506);
        map.put(2, 634);
        // case 1
        try {
            parser.parse("select * from db.user where id in (?,?,763)", map);
            throw new Error();
        } catch (AmbiguousRouteResultException e) {
        }
        // case 2
        SQLParsedResult parsedResult = parser.parse("select * from db.user where id in (?,?,762)", map);
        Assert.equals(parsedResult.getSql(), "SELECT * FROM db_02.user_0122 AS user WHERE id IN (?, ?, 762)");
        Set<String> expectedSchemas = new HashSet<>();
        expectedSchemas.add("db_02");
        Assert.isTrue(parsedResult.getSchemas().equals(expectedSchemas));
    }

    @Test
    public void testHitSdKeyWithBetween00() {
        ShardParser parser = buildParserForId();
        SQLParsedResult parsedResult = parser.parse("select * from db.user where id between 506 and 506", null);
        Assert.equals(parsedResult.getSql(), "SELECT * FROM db_02.user_0122 AS user WHERE id BETWEEN 506 AND 506");
        Set<String> expectedSchemas = new HashSet<>();
        expectedSchemas.add("db_02");
        Assert.isTrue(parsedResult.getSchemas().equals(expectedSchemas));
    }

    @Test
    public void testHitSdKeyWithBetween01() {
        ShardParser parser = buildParserForId();
        Map<Object, Object> map = new HashMap<>();
        map.put(1, 506);
        map.put(2, 634);
        map.put(3, 762);
        SQLParsedResult parsedResult = parser.parse("select * from db.user where id in (?,?,?)", map);
        Assert.equals(parsedResult.getSql(), "SELECT * FROM db_02.user_0122 AS user WHERE id IN (?, ?, ?)");
        Set<String> expectedSchemas = new HashSet<>();
        expectedSchemas.add("db_02");
        Assert.isTrue(parsedResult.getSchemas().equals(expectedSchemas));
    }

    @Test
    public void testHitSdKeyWithBetween02() {
        ShardParser parser = buildParserForId();
        Map<Object, Object> map = new HashMap<>();
        map.put(1, 506);
        map.put(2, 506);
        SQLParsedResult parsedResult = parser.parse("select * from db.user where id between ? and ?", map);
        Assert.equals(parsedResult.getSql(), "SELECT * FROM db_02.user_0122 AS user WHERE id BETWEEN ? AND ?");
        Set<String> expectedSchemas = new HashSet<>();
        expectedSchemas.add("db_02");
        Assert.isTrue(parsedResult.getSchemas().equals(expectedSchemas));
    }

    @Test
    public void testHitSdKeyWithBetween03() {
        ShardParser parser = buildParserForId();
        Map<Object, Object> map = new HashMap<>();
        map.put(1, 507);
        SQLParsedResult parsedResult = parser.parse("select * from db.user where id between 507 and ?", map);
        Assert.equals(parsedResult.getSql(), "SELECT * FROM db_03.user_0123 AS user WHERE id BETWEEN 507 AND ?");
        Set<String> expectedSchemas = new HashSet<>();
        expectedSchemas.add("db_03");
        Assert.isTrue(parsedResult.getSchemas().equals(expectedSchemas));
    }

    // =============== 分片列非命中查询 ================
    @Test
    public void testNotHitSdKeyWithEqual00() {
        ShardParser parser = buildParserForId();
        try {
            parser.parse("select * from db.user where id != 506", null);
            throw new Error();
        } catch (RuntimeException e) {
        }
    }

    @Test
    public void testNotHitSdKeyWithEqual01() {
        ShardParser parser = buildParserForId();
        SQLParsedResult parsedResult = parser.parse("select * from db.user where id != 506 and id = 507 ", null);
        Assert.equals(parsedResult.getSql(), "SELECT * FROM db_03.user_0123 AS user WHERE id != 506 AND id = 507");
    }

    @Test
    public void testNotHitSdKeyWithIn00() {
        ShardParser parser = buildParserForId();
        Map<Object, Object> map = new HashMap<>();
        map.put(1, 507);
        SQLParsedResult parsedResult = parser.parse("select * from db.user where id != 506 and id = ?", map);
        Assert.equals(parsedResult.getSql(), "SELECT * FROM db_03.user_0123 AS user WHERE id != 506 AND id = ?");
        Set<String> expectedSchemas = new HashSet<>();
        expectedSchemas.add("db_03");
        Assert.isTrue(parsedResult.getSchemas().equals(expectedSchemas));
    }

    @Test
    public void testNotHitSdKeyWithIn01() {
        try {
            ShardParser parser = buildParserForId();
            parser.parse("select * from db.user where id not in (506,634,762)", null);
            throw new Error();
        } catch (RuntimeException e) {
        }
    }

    @Test
    public void testNotHitSdKeyWithIn02() {
        ShardParser parser = buildParserForId();
        SQLParsedResult parsedResult = parser.parse("select * from db.user where id not in (506,634,762) and id in (507,635,763)",
                                                    null);
        Assert.equals(parsedResult.getSql(),
                      "SELECT * FROM db_03.user_0123 AS user WHERE id NOT IN (506, 634, 762) AND id IN (507, 635, 763)");
        Set<String> expectedSchemas = new HashSet<>();
        expectedSchemas.add("db_03");
        Assert.isTrue(parsedResult.getSchemas().equals(expectedSchemas));
    }

    @Test
    public void testNotHitSdKeyWithIn03() {
        ShardParser parser = buildParserForId();
        Map<Object, Object> map = new HashMap<>();
        map.put(1, 507);
        map.put(2, 635);
        map.put(3, 763);
        SQLParsedResult parsedResult = parser.parse(" select * from db.user where id not in (506,634,762) and id in (?,?,?)",
                                                    map);
        Assert.equals(parsedResult.getSql(),
                      "SELECT * FROM db_03.user_0123 AS user WHERE id NOT IN (506, 634, 762) AND id IN (?, ?, ?)");
        Set<String> expectedSchemas = new HashSet<>();
        expectedSchemas.add("db_03");
        Assert.isTrue(parsedResult.getSchemas().equals(expectedSchemas));
    }

    @Test
    public void testNotHitSdKeyWithBetween00() {
        try {
            ShardParser parser = buildParserForId();
            parser.parse("select * from db.user where id not between 506 and 506", null);
            throw new Error();
        } catch (RuntimeException e) {
        }
    }

    @Test
    public void testNotHitSdKeyWithBetween01() {
        ShardParser parser = buildParserForId();
        SQLParsedResult parsedResult = parser.parse("select * from db.user where id not between 506 and 506 and id between 507 and 507",
                                                    null);
        Assert.equals(parsedResult.getSql(),
                      "SELECT * FROM db_03.user_0123 AS user WHERE id NOT BETWEEN 506 AND 506 AND id BETWEEN 507 AND 507");
        Set<String> expectedSchemas = new HashSet<>();
        expectedSchemas.add("db_03");
        Assert.isTrue(parsedResult.getSchemas().equals(expectedSchemas));
    }

    @Test
    public void testNotHitSdKeyWithBetween02() {
        ShardParser parser = buildParserForId();
        Map<Object, Object> map = new HashMap<>();
        map.put(1, 507);
        map.put(2, 507);
        SQLParsedResult parsedResult = parser.parse("select * from db.user where id not between 506 and 506 and id between ? and ?",
                                                    map);
        Assert.equals(parsedResult.getSql(),
                      "SELECT * FROM db_03.user_0123 AS user WHERE id NOT BETWEEN 506 AND 506 AND id BETWEEN ? AND ?");
        Set<String> expectedSchemas = new HashSet<>();
        expectedSchemas.add("db_03");
        Assert.isTrue(parsedResult.getSchemas().equals(expectedSchemas));
    }

    // =============== 单表 ==================
    /**
     * sql参数
     */
    @Test
    public void testOneTabQuery00a() {
        ShardParser parser = buildParserForId();
        SQLParsedResult parsedResult = parser.parse("select * from db.user where id = 506 and name = 'allen'", null);
        Assert.equals(parsedResult.getSql(), "SELECT * FROM db_02.user_0122 AS user WHERE id = 506 AND name = 'allen'");
        Set<String> expectedSchemas = new HashSet<>();
        expectedSchemas.add("db_02");
        Assert.isTrue(parsedResult.getSchemas().equals(expectedSchemas));
    }

    @Test
    public void testOneTabQuery00b() {
        ShardParser parser = buildParserForName();
        SQLParsedResult parsedResult = parser.parse("select * from db.user where id = 506 and name = 'allen'", null);
        Assert.equals(parsedResult.getSql(), "SELECT * FROM db_02.user_0010 AS user WHERE id = 506 AND name = 'allen'");
        Set<String> expectedSchemas = new HashSet<>();
        expectedSchemas.add("db_02");
        Assert.isTrue(parsedResult.getSchemas().equals(expectedSchemas));
    }

    @Test
    public void testOneTabQuery01a() {
        ShardParser parser = buildParserForId();
        Map<Object, Object> map = new HashMap<Object, Object>();
        map.put(1, 506);
        map.put(2, "allen");
        map.put(3, "desc");
        SQLParsedResult parsedResult = parser.parse("select * from db.user where id= ? and name = ?", map);
        Assert.equals(parsedResult.getSql(), "SELECT * FROM db_02.user_0122 AS user WHERE id = ? AND name = ?");
        Set<String> expectedSchemas = new HashSet<>();
        expectedSchemas.add("db_02");
        Assert.isTrue(parsedResult.getSchemas().equals(expectedSchemas));
    }

    @Test
    public void testOneTabQuery01b() {
        ShardParser parser = buildParserForName();
        Map<Object, Object> map = new HashMap<Object, Object>();
        map.put(1, 506);
        map.put(2, "allen");
        map.put(3, "desc");
        SQLParsedResult parsedResult = parser.parse("select * from db.user where id= ? and name = ?", map);
        Assert.equals(parsedResult.getSql(), "SELECT * FROM db_02.user_0010 AS user WHERE id = ? AND name = ?");
        Set<String> expectedSchemas = new HashSet<>();
        expectedSchemas.add("db_02");
        Assert.isTrue(parsedResult.getSchemas().equals(expectedSchemas));
    }

    // =============== 多表 =================
    // 多表 - sql参数
    @Test
    public void testMultiTabQuery00a() {
        ShardParser parser = buildParserForId();
        SQLParsedResult parsedResult = parser.parse("select * from user, shop where user.id = 506 and shop.user_id = 507",
                                                    null);

        Assert.equals(parsedResult.getSql(),
                      "SELECT * FROM db_02.user_0122 AS user, db_03.shop_0123 AS shop WHERE user.id = 506 AND shop.user_id = 507");
        Set<String> expectedSchemas = new HashSet<>();
        expectedSchemas.add("db_02");
        expectedSchemas.add("db_03");
        Assert.isTrue(parsedResult.getSchemas().equals(expectedSchemas));
    }

    @Test
    public void testMultiTabQuery00b() {
        Map<Object, Object> map = new HashMap<Object, Object>();
        map.put(1, 506);
        map.put(2, 507);
        ShardParser parser = buildParserForId();
        SQLParsedResult parsedResult = parser.parse("select * from user, shop where user.id = ? and shop.user_id = ?",
                                                    map);
        Assert.equals(parsedResult.getSql(),
                      "SELECT * FROM db_02.user_0122 AS user, db_03.shop_0123 AS shop WHERE user.id = ? AND shop.user_id = ?");
        Set<String> expectedSchemas = new HashSet<>();
        expectedSchemas.add("db_02");
        expectedSchemas.add("db_03");
        Assert.isTrue(parsedResult.getSchemas().equals(expectedSchemas));
    }

    // join查询
    @Test
    public void testJoin00() {
        ShardParser parser = buildParserForId();
        SQLParsedResult parsedResult = parser.parse("select * from user left join shop on user.id=shop.user_id left join item on shop.id = item.shop_id where id = 506 and name = 'allen' and shop.user_id = 12 and item.user_id = 12",
                                                    null);
        Assert.equals(parsedResult.getSql(),
                      "SELECT * FROM db_02.user_0122 AS user LEFT JOIN db_04.shop_0012 AS shop ON user.id = shop.user_id LEFT JOIN db_04.item_0012 AS item"
                              + " ON shop.id = item.shop_id WHERE id = 506 AND name = 'allen' AND shop.user_id = 12 AND item.user_id = 12");
        Set<String> expectedSchemas = new HashSet<>();
        expectedSchemas.add("db_02");
        expectedSchemas.add("db_04");
        Assert.isTrue(parsedResult.getSchemas().equals(expectedSchemas));
    }

    @Test
    public void testJoin01() {
        ShardParser parser = buildParserForId();
        SQLParsedResult parsedResult = parser.parse("select * from user left join shop on user.id=shop.user_id left join item on shop.id = item.shop_id where id = 506 and name = 'allen' and shop.user_id = 12 and item.user_id = 12",
                                                    null);
        Assert.equals(parsedResult.getSql(),
                      "SELECT * FROM db_02.user_0122 AS user LEFT JOIN db_04.shop_0012 AS shop ON user.id = shop.user_id LEFT JOIN db_04.item_0012 AS item"
                              + " ON shop.id = item.shop_id WHERE id = 506 AND name = 'allen' AND shop.user_id = 12 AND item.user_id = 12");
        Set<String> expectedSchemas = new HashSet<>();
        expectedSchemas.add("db_02");
        expectedSchemas.add("db_04");
        Assert.isTrue(parsedResult.getSchemas().equals(expectedSchemas));
    }

    @Test
    public void testJoin02() {
        ShardParser parser = buildParserForNoRule();
        SQLParsedResult parsedResult = parser.parse("select * from user left join merchant on user.merchant_id = merchant.id where id = 506 "
                                                            + "and name = 'allen' and id in (select user_id from shop)",
                                                    null);
        Assert.equals(parsedResult.getSql(),
                      "SELECT * FROM db.user AS user LEFT JOIN merchant ON user.merchant_id = merchant.id WHERE id = 506 "
                              + "AND name = 'allen' AND id IN (SELECT user_id FROM db.shop AS shop)");
        Set<String> set = new HashSet<>();
        set.add("db");
        Assert.isTrue(parsedResult.getSchemas().equals(set));
    }

    // 子查询
    @Test
    public void testSubQuery00a() {
        ShardParser parser = buildParserForId();
        SQLParsedResult parsedResult = parser.parse("select * from db.user where id = 506 and name in (select user_id from shop where user_id = 507) ",
                                                    null);
        Assert.equals(parsedResult.getSql(),
                      "SELECT * FROM db_02.user_0122 AS user WHERE id = 506 AND name IN (SELECT user_id FROM db_03.shop_0123 AS shop WHERE user_id = 507)");
        Set<String> expectedSchemas = new HashSet<>();
        expectedSchemas.add("db_02");
        expectedSchemas.add("db_03");
        Assert.isTrue(parsedResult.getSchemas().equals(expectedSchemas));
    }

    @Test
    public void testSubQuery00b() {
        Map<Object, Object> map = new HashMap<Object, Object>();
        map.put(1, 506);
        map.put(2, 507);
        ShardParser parser = buildParserForId();
        SQLParsedResult parsedResult = parser.parse("select * from db.user where id = ? and name in (select user_id from shop where user_id = ?) ",
                                                    map);
        Assert.equals(parsedResult.getSql(),
                      "SELECT * FROM db_02.user_0122 AS user WHERE id = ? AND name IN (SELECT user_id FROM db_03.shop_0123 AS shop WHERE user_id = ?)");
        Set<String> expectedSchemas = new HashSet<>();
        expectedSchemas.add("db_02");
        expectedSchemas.add("db_03");
        Assert.isTrue(parsedResult.getSchemas().equals(expectedSchemas));
    }

    // ========================== 自定义路由 =========================

    @Test
    public void testNullValueForShardValue() {
        SimpleShardParser parser = buildParserForId();
        // 不支持分片字段未空
        try {
            parser.parse("select * from db.user where id = null  and name in (select user_id from shop where user_id = null) ",
                         null);
            throw new Error();
        } catch (Exception e) {
        }
        try {
            ShardRouteContext.setRouteInfo("db", "user", null);
            ShardRouteContext.setRouteInfo("db", "shop", null);
            parser.parse("select * from db.user where id != 506 and name in (select user_id from shop where user_id != 507) ",
                         null);
            throw new Error();
        } catch (Exception e) {
        }
    }

    @Test
    public void testShardRouteContext00a() {
        // 当指定了sdKey 时,只能使用RouteInfo进行路由
        SimpleShardParser parser = buildParserForId();

        try {
            ShardRouteContext.setRouteInfo("db", "user", 508);
            ShardRouteContext.setRouteInfo("db", "shop", 509);
            parser.parse("select * from db.user where id != 506 and name in (select user_id from shop where user_id != 507) ",
                         null);
            throw new Error();
        } catch (Exception e) {
        }
        ShardRouteInfo routeInfo1 = parser.getShardRouter().getRouteInfo("db", "user", 508);
        ShardRouteInfo routeInfo2 = parser.getShardRouter().getRouteInfo("db", "shop", 509);
        Assert.isTrue(routeInfo1.toString().equals("db_04.user_0124"));
        Assert.isTrue(routeInfo2.toString().equals("db_05.shop_0125"));

        ShardRouteContext.setRouteInfo("db", "user", routeInfo1);
        ShardRouteContext.setRouteInfo("db", "shop", routeInfo2);

        SQLParsedResult parsedResult = parser.parse("select * from db.user where id != 506 and name in (select user_id from shop where user_id != 507) ",
                                                    null);
        Assert.equals(parsedResult.getSql(),
                      "SELECT * FROM db_04.user_0124 AS user WHERE id != 506 AND name IN (SELECT user_id FROM db_05.shop_0125 AS shop WHERE user_id != 507)");
        Set<String> expectedSchemas = new HashSet<>();
        expectedSchemas.add("db_05");
        expectedSchemas.add("db_04");
        Assert.isTrue(parsedResult.getSchemas().equals(expectedSchemas));

    }

    @Test
    public void testShardRouteContext00b() {
        ShardRouteContext.setRouteInfo("db", "user", new ShardRouteInfo("db_07", "user_0125"));
        ShardRouteContext.setRouteInfo("db", "shop", new ShardRouteInfo("db_01", "shop_0123"));
        ShardParser parser = buildParserForId();
        SQLParsedResult parsedResult = parser.parse("select * from db.user where id != 507 AND name in (select user_id from shop where user_id != 507) ",
                                                    null);
        Assert.equals(parsedResult.getSql(),
                      "SELECT * FROM db_07.user_0125 AS user WHERE id != 507 AND name IN (SELECT user_id FROM db_01.shop_0123 AS shop WHERE user_id != 507)");
        Set<String> expectedSchemas = new HashSet<>();
        expectedSchemas.add("db_01");
        expectedSchemas.add("db_07");
        Assert.isTrue(parsedResult.getSchemas().equals(expectedSchemas));
    }

    @Test
    public void testShardRouteContext01a() {
        ShardRouteContext.setRouteInfo("db", "user", 508);
        ShardRouteContext.setRouteInfo("db", "shop", 509);
        ShardParser parser = buildParserForNoSdKey();
        SQLParsedResult parsedResult = parser.parse("select * from db.user where id != 506 and name in (select user_id from shop where user_id != 507) ",
                                                    null);
        Assert.equals(parsedResult.getSql(),
                      "SELECT * FROM db_04.user_0124 AS user WHERE id != 506 AND name IN (SELECT user_id FROM db_05.shop_0125 AS shop WHERE user_id != 507)");
        Set<String> expectedSchemas = new HashSet<>();
        expectedSchemas.add("db_04");
        expectedSchemas.add("db_05");
        Assert.isTrue(parsedResult.getSchemas().equals(expectedSchemas));
    }

    @Test
    public void testShardRouteContext01b() {
        ShardRouteContext.setRouteInfo("db", "user", new ShardRouteInfo("db_07", "user_0125"));
        ShardRouteContext.setRouteInfo("db", "shop", new ShardRouteInfo("db_01", "shop_0123"));
        ShardParser parser = buildParserForNoSdKey();
        SQLParsedResult parsedResult = parser.parse("select * from db.user where id != 507 AND name in (select user_id from shop where user_id != 507) ",
                                                    null);
        Assert.equals(parsedResult.getSql(),
                      "SELECT * FROM db_07.user_0125 AS user WHERE id != 507 AND name IN (SELECT user_id FROM db_01.shop_0123 AS shop WHERE user_id != 507)");
        Set<String> expectedSchemas = new HashSet<>();
        expectedSchemas.add("db_01");
        expectedSchemas.add("db_07");
        Assert.isTrue(parsedResult.getSchemas().equals(expectedSchemas));

    }

    // =========== no rule ==========
    @Test
    public void testNoRule00() {
        ShardRouteContext.setRouteInfo("db", "user", new ShardRouteInfo("db_07", "user_0125"));
        ShardRouteContext.setRouteInfo("db", "shop", new ShardRouteInfo("db_01", "shop_0123"));
        ShardParser parser = buildParserForNoRule();
        SQLParsedResult parsedResult = parser.parse("select * from db.user where id = 507 AND name in (select user_id from shop where user_id = 507) ",
                                                    null);
        Assert.equals(parsedResult.getSql(),
                      "SELECT * FROM db.user AS user WHERE id = 507 AND name IN (SELECT user_id FROM db.shop AS shop WHERE user_id = 507)");
        Set<String> expectedSchemas = new HashSet<>();
        expectedSchemas.add("db");
        Assert.isTrue(parsedResult.getSchemas().equals(expectedSchemas));
    }
}
