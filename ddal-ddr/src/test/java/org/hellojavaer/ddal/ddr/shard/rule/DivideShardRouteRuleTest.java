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

import org.hellojavaer.ddal.ddr.shard.RangeShardValue;
import org.hellojavaer.ddal.ddr.utils.Assert;
import org.junit.Test;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 18/09/2017.
 */
public class DivideShardRouteRuleTest {

    @Test
    public void test01() {
        DivideShardRouteRule rule = new DivideShardRouteRule(8L, 4L);
        Assert.equals(rule.parseScName("member", 13), "member_1");
        Assert.equals(rule.parseTbName("user", 13), "user_3");
    }

    @Test
    public void test02() {
        DivideShardRouteRule rule = new DivideShardRouteRule(8L, 4L);
        Assert.equals(rule.parseScName("member", new RangeShardValue(0L, 3L)), "member_0");
        Assert.equals(rule.parseTbName("user", new RangeShardValue(0L, 3L)), "user_0");

        DivideShardRouteRule rule1 = new DivideShardRouteRule(8L, 4L);
        Assert.equals(rule1.parseScName("member", new RangeShardValue(4L, 7L)), "member_0");
        Assert.equals(rule1.parseTbName("user", new RangeShardValue(4L, 7L)), "user_1");
    }

}
