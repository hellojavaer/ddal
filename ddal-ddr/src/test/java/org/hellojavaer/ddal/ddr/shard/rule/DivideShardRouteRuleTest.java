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
package org.hellojavaer.ddal.ddr.shard.rule;

import org.hellojavaer.ddal.core.utils.Assert;
import org.hellojavaer.ddal.ddr.shard.RangeShardValue;
import org.hellojavaer.ddal.ddr.shard.ShardRouteInfo;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 18/09/2017.
 */
public class DivideShardRouteRuleTest {

    @Test
    public void test01() {
        DivideShardRouteRule rule = new DivideShardRouteRule(8L, 4L);
        for (int i = 0; i < 8; i++) {
            Assert.equals(rule.parseScName("member", i), "member_0");
        }
        for (int i = 8; i < 16; i++) {
            Assert.equals(rule.parseScName("member", i), "member_1");
        }
        for (int i = 0; i < 4; i++) {
            Assert.equals(rule.parseTbName("user", i), "user_0");
        }
        for (int i = 4; i < 8; i++) {
            Assert.equals(rule.parseTbName("user", i), "user_1");
        }
    }

    @Test
    public void test02() {
        DivideShardRouteRule rule = new DivideShardRouteRule(8L, 4L, true);
        for (int i = 0; i < 8; i++) {
            Assert.equals(rule.parseScName("member", i), "member");
        }
        for (int i = 8; i < 16; i++) {
            Assert.equals(rule.parseScName("member", i), "member_1");
        }
        for (int i = 0; i < 4; i++) {
            Assert.equals(rule.parseTbName("user", i), "user");
        }
        for (int i = 4; i < 8; i++) {
            Assert.equals(rule.parseTbName("user", i), "user_1");
        }
    }

    @Test
    public void test03() {
        DivideShardRouteRule rule = new DivideShardRouteRule(8L, 4L);
        Map<ShardRouteInfo, List<RangeShardValue>> map = rule.groupSdValuesByRouteInfo("member", "user",
                                                                                       new RangeShardValue(2l, 7l));
        Map<ShardRouteInfo, List<RangeShardValue>> expectedResult = new LinkedHashMap<>();
        expectedResult.put(new ShardRouteInfo("member_0", "user_0"), Arrays.asList(new RangeShardValue(2l, 3l)));
        expectedResult.put(new ShardRouteInfo("member_0", "user_1"), Arrays.asList(new RangeShardValue(4l, 7l)));
        Assert.equals(map, expectedResult);

        //
        rule = new DivideShardRouteRule(null, 4L);
        map = rule.groupSdValuesByRouteInfo("member", "user", new RangeShardValue(2l, 7l));
        expectedResult = new LinkedHashMap<>();
        expectedResult.put(new ShardRouteInfo("member", "user_0"), Arrays.asList(new RangeShardValue(2l, 3l)));
        expectedResult.put(new ShardRouteInfo("member", "user_1"), Arrays.asList(new RangeShardValue(4l, 7l)));
        Assert.equals(map, expectedResult);

        //
        rule = new DivideShardRouteRule(8l, null);
        map = rule.groupSdValuesByRouteInfo("member", "user", new RangeShardValue(2l, 7l));
        expectedResult = new LinkedHashMap<>();
        expectedResult.put(new ShardRouteInfo("member_0", "user"), Arrays.asList(new RangeShardValue(2l, 7l)));
        Assert.equals(map, expectedResult);

        //
        rule = new DivideShardRouteRule(null, null);
        map = rule.groupSdValuesByRouteInfo("member", "user", new RangeShardValue(2l, 7l));
        expectedResult = new LinkedHashMap<>();
        expectedResult.put(new ShardRouteInfo("member", "user"), Arrays.asList(new RangeShardValue(2l, 7l)));
        Assert.equals(map, expectedResult);
    }

    @Test
    public void test04() {
        DivideShardRouteRule rule = new DivideShardRouteRule(8L, 4L, true);
        Map<ShardRouteInfo, List<RangeShardValue>> map = rule.groupSdValuesByRouteInfo("member", "user",
                                                                                       new RangeShardValue(2l, 7l));
        Map<ShardRouteInfo, List<RangeShardValue>> expectedResult = new LinkedHashMap<>();
        expectedResult.put(new ShardRouteInfo("member", "user"), Arrays.asList(new RangeShardValue(2l, 3l)));
        expectedResult.put(new ShardRouteInfo("member", "user_1"), Arrays.asList(new RangeShardValue(4l, 7l)));
        Assert.equals(map, expectedResult);

        //
        rule = new DivideShardRouteRule(null, 4L, true);
        map = rule.groupSdValuesByRouteInfo("member", "user", new RangeShardValue(2l, 7l));
        expectedResult = new LinkedHashMap<>();
        expectedResult.put(new ShardRouteInfo("member", "user"), Arrays.asList(new RangeShardValue(2l, 3l)));
        expectedResult.put(new ShardRouteInfo("member", "user_1"), Arrays.asList(new RangeShardValue(4l, 7l)));
        Assert.equals(map, expectedResult);

        //
        rule = new DivideShardRouteRule(8l, null, true);
        map = rule.groupSdValuesByRouteInfo("member", "user", new RangeShardValue(2l, 7l));
        expectedResult = new LinkedHashMap<>();
        expectedResult.put(new ShardRouteInfo("member", "user"), Arrays.asList(new RangeShardValue(2l, 7l)));
        Assert.equals(map, expectedResult);

        //
        rule = new DivideShardRouteRule(null, null, true);
        map = rule.groupSdValuesByRouteInfo("member", "user", new RangeShardValue(2l, 7l));
        expectedResult = new LinkedHashMap<>();
        expectedResult.put(new ShardRouteInfo("member", "user"), Arrays.asList(new RangeShardValue(2l, 7l)));
        Assert.equals(map, expectedResult);
    }

}
