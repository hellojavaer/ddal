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

import org.hellojavaer.ddal.ddr.shard.simple.SimpleShardParser;
import org.hellojavaer.ddal.ddr.shard.simple.SimpleShardRouteRuleBinding;
import org.hellojavaer.ddal.ddr.shard.simple.SimpleShardRouter;
import org.hellojavaer.ddal.ddr.shard.rule.SpelShardRouteRule;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * 构建数据模型
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 18/12/2016.
 */
public class BaseTestShardParser {

    /**
     * 构造数据模型
     * @return
     */
    protected SimpleShardParser buildShardParser() {
        List<SimpleShardRouteRuleBinding> bindings = new ArrayList<SimpleShardRouteRuleBinding>();
        // 定义规则
        SpelShardRouteRule numRule = new SpelShardRouteRule("{scName}_{format('%02d', sdValue % 8)}",
                                                            "{tbName}_{format('%04d', sdValue % 128)}");
        SpelShardRouteRule strRule = new SpelShardRouteRule("{scName}_{format('%02d', sdValue.hashCode() % 8)}",
                                                            "{tbName}_{format('%04d', sdValue.hashCode() % 128)}");
        // 用户表
        SimpleShardRouteRuleBinding user = new SimpleShardRouteRuleBinding();
        user.setScName("db");
        user.setTbName("user");
        user.setSdKey("id");
        user.setSdValues("[0..127]");
        user.setRule(numRule);
        bindings.add(user);

        // 商铺表
        SimpleShardRouteRuleBinding shop = new SimpleShardRouteRuleBinding();
        shop.setScName("db");
        shop.setTbName("shop");
        shop.setSdKey("user_id");
        shop.setSdValues("[0..127]");
        shop.setRule(numRule);
        bindings.add(shop);

        // 商品表
        SimpleShardRouteRuleBinding item = new SimpleShardRouteRuleBinding();
        item.setScName("db");
        item.setTbName("item");
        item.setSdKey("user_id");
        item.setSdValues("[0..127]");
        item.setRule(numRule);
        bindings.add(item);

        // 商品异构表
        SimpleShardRouteRuleBinding item_1 = new SimpleShardRouteRuleBinding();
        item_1.setScName("db_x");
        item_1.setTbName("item");
        item_1.setSdKey("shop_id");
        item_1.setSdValues("[0..127]");
        item_1.setRule(numRule);
        bindings.add(item_1);

        // 用户副表
        SimpleShardRouteRuleBinding user_back = new SimpleShardRouteRuleBinding();
        user_back.setScName("db_back");
        user_back.setTbName("user");
        user_back.setSdKey("name");
        user_back.setSdValues("[1..128]");
        user_back.setRule(strRule);
        bindings.add(user_back);

        SimpleShardRouter shardRouter = new SimpleShardRouter(bindings);
        SimpleShardParser parser = new SimpleShardParser(null, shardRouter);
        return parser;
    }

}
