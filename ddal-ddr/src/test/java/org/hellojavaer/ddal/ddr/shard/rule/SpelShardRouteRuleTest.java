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
package org.hellojavaer.ddal.ddr.shard.rule;

import org.hellojavaer.ddal.core.utils.Assert;
import org.hellojavaer.ddal.ddr.shard.RangeShardValue;
import org.hellojavaer.ddal.ddr.shard.ShardRouteInfo;
import org.hellojavaer.ddal.ddr.shard.simple.SimpleShardRouteRuleBinding;
import org.hellojavaer.ddal.ddr.shard.simple.SimpleShardRouter;
import org.junit.Test;

import java.util.*;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 25/04/2017.
 */
public class SpelShardRouteRuleTest {

    @Test
    public void test01() {
        SpelShardRouteRule rule = new SpelShardRouteRule("{scName}_{#format('%02d', sdValue % 4)}",
                                                         "{tbName}_{#format('%04d', sdValue % 8)}");
        Assert.equals(rule.parseScName("member", 10101), "member_01");
        Assert.equals(rule.parseTbName("user", 10101), "user_0005");
    }

    @Test
    public void test02() {
        List<String> expectedResult = new ArrayList();
        expectedResult.add("db_00.user_0000");
        expectedResult.add("db_01.user_0001");
        expectedResult.add("db_02.user_0002");
        expectedResult.add("db_03.user_0003");
        expectedResult.add("db_00.user_0004");
        expectedResult.add("db_01.user_0005");
        expectedResult.add("db_02.user_0006");
        expectedResult.add("db_03.user_0007");
        expectedResult.add("db_00.user_0008");
        expectedResult.add("db_01.user_0009");
        expectedResult.add("db_02.user_0010");
        expectedResult.add("db_03.user_0011");
        expectedResult.add("db_00.user_0012");
        expectedResult.add("db_01.user_0013");
        expectedResult.add("db_02.user_0014");
        expectedResult.add("db_03.user_0015");
        List<SimpleShardRouteRuleBinding> bindings = new ArrayList<SimpleShardRouteRuleBinding>();
        SpelShardRouteRule numRule = new SpelShardRouteRule("{scName}_{format('%02d', sdValue % 4)}",
                                                            "{tbName}_{format('%04d', sdValue % 16)}");
        SimpleShardRouteRuleBinding user = new SimpleShardRouteRuleBinding();
        user.setScName("db");
        user.setTbName("user");
        user.setSdKey("id");
        user.setSdValues("[0..15]");
        user.setRule(numRule);
        bindings.add(user);
        SimpleShardRouter shardRouter = new SimpleShardRouter(bindings);
        List<ShardRouteInfo> routeInfos = shardRouter.getRouteInfos("db", "user");
        int count = 0;
        for (ShardRouteInfo si : routeInfos) {
            Assert.equals(si.toString(), expectedResult.get(count));
            count++;
        }
    }

    @Test
    public void test03() {
        List<String> expectedResult = new ArrayList();
        expectedResult.add("db_index_01.user_0001");
        expectedResult.add("db_index_02.user_0002");
        expectedResult.add("db_index_03.user_0003");
        expectedResult.add("db_index_00.user_0004");
        expectedResult.add("db_index_01.user_0005");
        expectedResult.add("db_index_02.user_0006");
        expectedResult.add("db_index_03.user_0007");
        expectedResult.add("db_index_00.user_0008");
        expectedResult.add("db_index_01.user_0009");
        expectedResult.add("db_index_02.user_0010");
        expectedResult.add("db_index_03.user_0011");
        expectedResult.add("db_index_00.user_0012");
        expectedResult.add("db_index_01.user_0013");
        expectedResult.add("db_index_02.user_0014");
        expectedResult.add("db_index_03.user_0015");
        expectedResult.add("db_index_00.user_0000");
        List<SimpleShardRouteRuleBinding> bindings = new ArrayList<SimpleShardRouteRuleBinding>();
        SpelShardRouteRule strRule = new SpelShardRouteRule("{scName}_{format('%02d', sdValue.hashCode() % 4)}",
                                                            "{tbName}_{format('%04d', sdValue.hashCode() % 16)}");
        SimpleShardRouteRuleBinding user = new SimpleShardRouteRuleBinding();
        user.setScName("db_index");
        user.setTbName("user");
        user.setSdKey("name");
        user.setSdValues("[1..16]");
        user.setRule(strRule);
        bindings.add(user);
        SimpleShardRouter shardRouter = new SimpleShardRouter(bindings);
        List<ShardRouteInfo> routeInfos = shardRouter.getRouteInfos("db_index", "user");
        int count = 0;
        for (ShardRouteInfo si : routeInfos) {
            Assert.equals(si.toString(), expectedResult.get(count));
            count++;
        }
    }

    @Test
    public void test04() {
        SpelShardRouteRule strRule = new SpelShardRouteRule("{scName}_{format('%02d', sdValue.hashCode() % 4)}",
                                                            "{tbName}_{format('%04d', sdValue.hashCode() % 16)}");
        Map<ShardRouteInfo, List<RangeShardValue>> map = strRule.groupSdValuesByRouteInfo("db", "user",
                                                                                          new RangeShardValue(0L, 31L));
        Map<ShardRouteInfo, List<RangeShardValue>> r = new LinkedHashMap<>();
        r.put(new ShardRouteInfo("db_00", "user_0000"),
              Arrays.asList(new RangeShardValue(0L, 0L), new RangeShardValue(16L, 16L)));
        r.put(new ShardRouteInfo("db_01", "user_0001"),
              Arrays.asList(new RangeShardValue(1L, 1L), new RangeShardValue(17L, 17L)));
        r.put(new ShardRouteInfo("db_02", "user_0002"),
              Arrays.asList(new RangeShardValue(2L, 2L), new RangeShardValue(18L, 18L)));
        r.put(new ShardRouteInfo("db_03", "user_0003"),
              Arrays.asList(new RangeShardValue(3L, 3L), new RangeShardValue(19L, 19L)));
        r.put(new ShardRouteInfo("db_00", "user_0004"),
              Arrays.asList(new RangeShardValue(4L, 4L), new RangeShardValue(20L, 20L)));
        r.put(new ShardRouteInfo("db_01", "user_0005"),
              Arrays.asList(new RangeShardValue(5L, 5L), new RangeShardValue(21L, 21L)));
        r.put(new ShardRouteInfo("db_02", "user_0006"),
              Arrays.asList(new RangeShardValue(6L, 6L), new RangeShardValue(22L, 22L)));
        r.put(new ShardRouteInfo("db_03", "user_0007"),
              Arrays.asList(new RangeShardValue(7L, 7L), new RangeShardValue(23L, 23L)));
        r.put(new ShardRouteInfo("db_00", "user_0008"),
              Arrays.asList(new RangeShardValue(8L, 8L), new RangeShardValue(24L, 24L)));
        r.put(new ShardRouteInfo("db_01", "user_0009"),
              Arrays.asList(new RangeShardValue(9L, 9L), new RangeShardValue(25L, 25L)));
        r.put(new ShardRouteInfo("db_02", "user_0010"),
              Arrays.asList(new RangeShardValue(10L, 10L), new RangeShardValue(26L, 26L)));
        r.put(new ShardRouteInfo("db_03", "user_0011"),
              Arrays.asList(new RangeShardValue(11L, 11L), new RangeShardValue(27L, 27L)));
        r.put(new ShardRouteInfo("db_00", "user_0012"),
              Arrays.asList(new RangeShardValue(12L, 12L), new RangeShardValue(28L, 28L)));
        r.put(new ShardRouteInfo("db_01", "user_0013"),
              Arrays.asList(new RangeShardValue(13L, 13L), new RangeShardValue(29L, 29L)));
        r.put(new ShardRouteInfo("db_02", "user_0014"),
              Arrays.asList(new RangeShardValue(14L, 14L), new RangeShardValue(30L, 30L)));
        r.put(new ShardRouteInfo("db_03", "user_0015"),
              Arrays.asList(new RangeShardValue(15L, 15L), new RangeShardValue(31L, 31L)));
        Assert.equals(map, r);
    }

    @Test
    public void test05() {
        SpelShardRouteRule strRule = new SpelShardRouteRule(null, null);
        Map<ShardRouteInfo, List<RangeShardValue>> map = strRule.groupSdValuesByRouteInfo("db", "user",
                                                                                          new RangeShardValue(0L, 131L));
        Map<ShardRouteInfo, List<RangeShardValue>> r = new LinkedHashMap<>();
        r.put(new ShardRouteInfo("db", "user"), Arrays.asList(new RangeShardValue(0L, 131L)));
        Assert.equals(map, r);
    }

}
