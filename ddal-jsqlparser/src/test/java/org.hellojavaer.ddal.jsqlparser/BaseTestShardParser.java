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

import org.hellojavaer.ddal.ddr.shard.ShardParser;
import org.hellojavaer.ddal.ddr.shard.simple.SimpleShardParser;
import org.hellojavaer.ddal.ddr.shard.simple.SimpleShardRouteRuleBinding;
import org.hellojavaer.ddal.ddr.shard.simple.SimpleShardRouter;
import org.hellojavaer.ddal.ddr.shard.rule.SpelShardRouteRule;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * build data model
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 18/12/2016.
 */
public class BaseTestShardParser {

    /**
     * user -1:N- shop -1:N- item  
     */
    protected SimpleShardParser buildParserForId() {
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
        user.setSdValues("[1..128]");
        user.setRule(numRule);
        bindings.add(user);

        // 商铺表
        SimpleShardRouteRuleBinding shop = new SimpleShardRouteRuleBinding();
        shop.setScName("db");
        shop.setTbName("shop");
        shop.setSdKey("user_id");
        shop.setSdValues("[1..128]");
        shop.setRule(numRule);
        bindings.add(shop);

        // 商品表
        SimpleShardRouteRuleBinding item = new SimpleShardRouteRuleBinding();
        item.setScName("db");
        item.setTbName("item");
        item.setSdKey("user_id");
        item.setSdValues("[1..128]");
        item.setRule(numRule);
        bindings.add(item);

        SimpleShardRouter shardRouter = new SimpleShardRouter(bindings);
        SimpleShardParser parser = new SimpleShardParser(new JSQLParser(), shardRouter);
        return parser;
    }

    protected ShardParser buildParserForName() {
        List<SimpleShardRouteRuleBinding> bindings = new ArrayList<SimpleShardRouteRuleBinding>();
        SimpleShardRouteRuleBinding b0 = new SimpleShardRouteRuleBinding();
        b0.setScName("db");
        b0.setTbName("user");
        b0.setSdKey("name");
        b0.setSdValues("[1..128]");

        SpelShardRouteRule r0 = new SpelShardRouteRule("{scName}_{format('%02d', sdValue.hashCode() % 8)}",
                                                       "{tbName}_{format('%04d', sdValue.hashCode() % 128)}");
        b0.setRule(r0);
        bindings.add(b0);
        SimpleShardRouter shardRouter = new SimpleShardRouter(bindings);
        SimpleShardParser parser = new SimpleShardParser(new JSQLParser(), shardRouter);
        return parser;
    }

    protected ShardParser buildParserForNoSdKey() {
        SpelShardRouteRule r0 = new SpelShardRouteRule("{scName}_{format('%02d', sdValue.hashCode() % 8)}",
                                                       "{tbName}_{format('%04d', sdValue.hashCode() % 128)}");
        List<SimpleShardRouteRuleBinding> bindings = new ArrayList<SimpleShardRouteRuleBinding>();
        SimpleShardRouteRuleBinding user = new SimpleShardRouteRuleBinding();
        user.setScName("db");
        user.setTbName("user");
        user.setRule(r0);
        bindings.add(user);

        SimpleShardRouteRuleBinding shop = new SimpleShardRouteRuleBinding();
        shop.setScName("db");
        shop.setTbName("shop");
        shop.setRule(r0);
        bindings.add(shop);

        SimpleShardRouteRuleBinding item = new SimpleShardRouteRuleBinding();
        item.setScName("db");
        item.setTbName("item");
        item.setRule(r0);
        bindings.add(item);

        SimpleShardRouter shardRouter = new SimpleShardRouter(bindings);
        SimpleShardParser parser = new SimpleShardParser(new JSQLParser(), shardRouter);
        return parser;
    }

    protected ShardParser buildParserForNoRule() {
        List<SimpleShardRouteRuleBinding> bindings = new ArrayList<SimpleShardRouteRuleBinding>();
        SimpleShardRouteRuleBinding b0 = new SimpleShardRouteRuleBinding();
        b0.setScName("db");
        b0.setTbName("user");

        SimpleShardRouteRuleBinding b1 = new SimpleShardRouteRuleBinding();
        b1.setScName("db");
        b1.setTbName("shop");

        bindings.add(b0);
        bindings.add(b1);
        SimpleShardRouter shardRouter = new SimpleShardRouter(bindings);
        SimpleShardParser parser = new SimpleShardParser(new JSQLParser(), shardRouter);
        return parser;
    }
}
