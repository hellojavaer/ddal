package org.hellojavaer.ddal.ddr.shard;

import org.hellojavaer.ddal.ddr.shard.simple.SpelShardRouteRule;
import org.hellojavaer.ddal.ddr.utils.Assert;
import org.junit.Test;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 25/04/2017.
 */
public class SpelShardRouteRuleTest {

    @Test
    public void test01() {
        SpelShardRouteRule rule = new SpelShardRouteRule();
        rule.setScRouteRule("{#scName}_{#format('%02d', #sdValue % 4)}");
        rule.setTbRouteRule("{#tbName}_{#format('%04d', #sdValue % 8)}");
        ShardRouteRuleContext context = new ShardRouteRuleContext();
        context.setScName("member");
        context.setTbName("user");
        context.setSdValue(10101);
        Assert.equals(rule.parseScName(context), "member_01");
        Assert.equals(rule.parseTbName(context), "user_0005");
    }

}
