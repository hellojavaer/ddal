/*
 * Copyright 2018-2018 the original author or authors.
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
package org.hellojavaer.ddal.ddr.expression;

import org.hellojavaer.ddal.core.utils.Assert;
import org.junit.Test;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 2018/5/28.
 */
public class ShardRouteRuleExpressionContextTest {

    @Test
    public void test00() {
        Assert.notNull(ShardRouteRuleExpressionContext.lookupVariable("format"));
        Assert.equals(ShardRouteRuleExpressionContext.getVariable("format"), null);
        //
        ShardRouteRuleExpressionContext.setVariable("a", "1");
        Assert.equals(ShardRouteRuleExpressionContext.getVariable("a"), "1");
        Assert.equals(ShardRouteRuleExpressionContext.lookupVariable("a"), "1");
        //
        ShardRouteRuleExpressionContext.pushContext();
        Assert.equals(ShardRouteRuleExpressionContext.getVariable("a"), null);
        Assert.equals(ShardRouteRuleExpressionContext.lookupVariable("a"), "1");
        //
        ShardRouteRuleExpressionContext.setVariable("a", "2");
        Assert.equals(ShardRouteRuleExpressionContext.getVariable("a"), "2");
        Assert.equals(ShardRouteRuleExpressionContext.lookupVariable("a"), "2");
        //
        ShardRouteRuleExpressionContext.popContext();
        Assert.equals(ShardRouteRuleExpressionContext.getVariable("a"), "1");
        Assert.equals(ShardRouteRuleExpressionContext.lookupVariable("a"), "1");
    }

}
