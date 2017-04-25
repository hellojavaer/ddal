package org.hellojavaer.ddal.ddr.shard;

import org.hellojavaer.ddal.ddr.shard.simple.SimpleShardParser;
import org.hellojavaer.ddal.ddr.shard.simple.SimpleShardRouteRuleBinding;
import org.hellojavaer.ddal.ddr.shard.simple.SimpleShardRouter;
import org.hellojavaer.ddal.ddr.shard.simple.SpelShardRouteRule;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * 构建数据模型
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 18/12/2016.
 */
public class BaseShardParserTest {

    /**
     * 构造数据模型
     * @return
     */
    protected ShardParser buildParserForId() {
        SimpleShardParser parser = new SimpleShardParser();
        SimpleShardRouter shardRouter = new SimpleShardRouter();
        List<SimpleShardRouteRuleBinding> bindings = new ArrayList<SimpleShardRouteRuleBinding>();
        // 定义规则
        SpelShardRouteRule numRule = new SpelShardRouteRule();
        numRule.setScRouteRule("{#scName}_{#format('%02d', #sdValue % 8)}");
        numRule.setTbRouteRule("{#tbName}_{#format('%04d', #sdValue % 128)}");

        SpelShardRouteRule strRule = new SpelShardRouteRule();
        strRule.setScRouteRule("{#scName}_{#format('%02d', #sdValue.hashCode() % 8)}");
        strRule.setTbRouteRule("{#tbName}_{#format('%04d', #sdValue.hashCode() % 128)}");

        // 用户表
        SimpleShardRouteRuleBinding user = new SimpleShardRouteRuleBinding();
        user.setScName("db");
        user.setTbName("user");
        user.setSdKey("id");
        user.setSdValues("[0~127]");
        user.setRule(numRule);
        bindings.add(user);

        // 商铺表
        SimpleShardRouteRuleBinding shop = new SimpleShardRouteRuleBinding();
        shop.setScName("db");
        shop.setTbName("shop");
        shop.setSdKey("user_id");
        shop.setSdValues("[0~127]");
        shop.setRule(numRule);
        bindings.add(shop);

        // 商品表
        SimpleShardRouteRuleBinding item = new SimpleShardRouteRuleBinding();
        item.setScName("db");
        item.setTbName("item");
        item.setSdKey("user_id");
        item.setSdValues("[0~127]");
        item.setRule(numRule);
        bindings.add(item);

        // 商品异构表
        SimpleShardRouteRuleBinding item_1 = new SimpleShardRouteRuleBinding();
        item_1.setScName("db_x");
        item_1.setTbName("item");
        item_1.setSdKey("shop_id");
        item_1.setSdValues("[0~127]");
        item_1.setRule(numRule);
        bindings.add(item_1);

        // 用户副表
        // SimpleShardingRouteRuleBinding user_back = new SimpleShardingRouteRuleBinding();
        // user_back.setScName("db_back");
        // user_back.setTbName("user");
        // user_back.setSdKey("name");
        // user_back.setSdValues("[1~128]");
        // user_back.setRule(numRule);
        // bindings.add(user_back);

        shardRouter.setRouteRuleBindings(bindings);
        parser.setShardRouter(shardRouter);
        parser.setSqlParser(null);
        return parser;
    }

}
