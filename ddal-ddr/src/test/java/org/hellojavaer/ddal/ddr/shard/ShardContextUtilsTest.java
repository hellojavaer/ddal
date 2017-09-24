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
package org.hellojavaer.ddal.ddr.shard;

import org.hellojavaer.ddal.core.utils.Assert;
import org.hellojavaer.ddal.ddr.shard.rule.SpelShardRouteRule;
import org.hellojavaer.ddal.ddr.shard.simple.SimpleShardRouteRuleBinding;
import org.hellojavaer.ddal.ddr.shard.simple.SimpleShardRouter;
import org.junit.Test;

import java.util.*;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 22/06/2017.
 */
public class ShardContextUtilsTest {

    @Test
    public void groupSdValuesByRouteInfo() {
        List<SimpleShardRouteRuleBinding> bindings = new ArrayList<>();
        SpelShardRouteRule numRule = new SpelShardRouteRule("{scName}_{format('%02d', sdValue % 4)}",
                                                            "{tbName}_{format('%04d', sdValue % 16)}");
        SimpleShardRouteRuleBinding user = new SimpleShardRouteRuleBinding();
        user.setScName("db");
        user.setTbName("user");
        user.setSdKey("id");
        user.setSdValues("[0..31]");
        user.setRule(numRule);
        bindings.add(user);
        SimpleShardRouter shardRouter = new SimpleShardRouter(bindings);

        List<Long> list = new ArrayList<>();
        int testDataLength = 32;
        for (int i = 0; i < testDataLength; i++) {
            list.add((long) i);
        }

        Map<ShardRouteInfo, List<Long>> expectedResult = new LinkedHashMap<>();
        expectedResult.put(new ShardRouteInfo("db_00", "user_0000"), Arrays.asList(0L, 16L));
        expectedResult.put(new ShardRouteInfo("db_01", "user_0001"), Arrays.asList(1L, 17L));
        expectedResult.put(new ShardRouteInfo("db_02", "user_0002"), Arrays.asList(2L, 18L));
        expectedResult.put(new ShardRouteInfo("db_03", "user_0003"), Arrays.asList(3L, 19L));
        expectedResult.put(new ShardRouteInfo("db_00", "user_0004"), Arrays.asList(4L, 20L));
        expectedResult.put(new ShardRouteInfo("db_01", "user_0005"), Arrays.asList(5L, 21L));
        expectedResult.put(new ShardRouteInfo("db_02", "user_0006"), Arrays.asList(6L, 22L));
        expectedResult.put(new ShardRouteInfo("db_03", "user_0007"), Arrays.asList(7L, 23L));
        expectedResult.put(new ShardRouteInfo("db_00", "user_0008"), Arrays.asList(8L, 24L));
        expectedResult.put(new ShardRouteInfo("db_01", "user_0009"), Arrays.asList(9L, 25L));
        expectedResult.put(new ShardRouteInfo("db_02", "user_0010"), Arrays.asList(10L, 26L));
        expectedResult.put(new ShardRouteInfo("db_03", "user_0011"), Arrays.asList(11L, 27L));
        expectedResult.put(new ShardRouteInfo("db_00", "user_0012"), Arrays.asList(12L, 28L));
        expectedResult.put(new ShardRouteInfo("db_01", "user_0013"), Arrays.asList(13L, 29L));
        expectedResult.put(new ShardRouteInfo("db_02", "user_0014"), Arrays.asList(14L, 30L));
        expectedResult.put(new ShardRouteInfo("db_03", "user_0015"), Arrays.asList(15L, 31L));

        Map<ShardRouteInfo, List<Long>> map = ShardRouteUtils.groupSdValuesByRouteInfo(shardRouter, "db", "user", list);
        Assert.equals(map, expectedResult);

        Set<Long> set = new LinkedHashSet<>();
        for (int i = 0; i < 32; i++) {
            set.add((long) i);
        }
        Map<ShardRouteInfo, Set<Long>> map1 = ShardRouteUtils.groupSdValuesByRouteInfo(shardRouter, "db", "user", set);
        Assert.equals(map1, expectedResult);

    }
}
