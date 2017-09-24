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
package org.hellojavaer.ddal.example.example0.init;

import org.hellojavaer.ddal.ddr.datasource.WeightedDataSource;
import org.hellojavaer.ddal.ddr.datasource.jdbc.DefaultDDRDataSource;
import org.hellojavaer.ddal.ddr.datasource.manager.rw.DefaultReadWriteDataSourceManager;
import org.hellojavaer.ddal.ddr.datasource.manager.rw.ReadOnlyDataSourceBinding;
import org.hellojavaer.ddal.ddr.datasource.manager.rw.WriteOnlyDataSourceBinding;
import org.hellojavaer.ddal.ddr.shard.ShardRouteInfo;
import org.hellojavaer.ddal.ddr.shard.ShardRouteUtils;
import org.hellojavaer.ddal.ddr.shard.ShardRouter;
import org.hellojavaer.ddal.ddr.shard.simple.SimpleShardParser;
import org.hellojavaer.ddal.ddr.shard.simple.SimpleShardRouteRuleBinding;
import org.hellojavaer.ddal.ddr.shard.simple.SimpleShardRouter;
import org.hellojavaer.ddal.ddr.shard.rule.SpelShardRouteRule;
import org.hellojavaer.ddal.jsqlparser.JSQLParser;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 12/07/2017.
 */
public class InitDatabaseSqlBuilder {

    // run this method
    public static void main(String[] args) {
        ShardRouter shardRouter = buildShardRouter();
        String createDatabaseSql = "CREATE DATABASE IF NOT EXISTS `%s` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;";
        String createTableSql = "CREATE TABLE `%s`(`id` bigint(20) NOT NULL, `name` varchar(32) NOT NULL, PRIMARY KEY (`id`), KEY `idx_name` (`name`) USING BTREE ) ENGINE=InnoDB;";
        build(shardRouter, createDatabaseSql, createTableSql, "base", "user");
    }

    public static void build(ShardRouter shardRouter, String createDatabaseSql, String createTableSql, String scName,
                             String tbName) {
        Map<String, List<ShardRouteInfo>> groupedRouteInfos = ShardRouteUtils.groupRouteInfosByScName(shardRouter.getRouteInfos(scName,
                                                                                                                           tbName));
        boolean first = true;
        for (Map.Entry<String, List<ShardRouteInfo>> entry : groupedRouteInfos.entrySet()) {
            System.out.println();
            System.out.println(String.format(createDatabaseSql, entry.getKey()));// create db
            System.out.println("use " + entry.getKey() + ";");// create db
            if (first) {
                // create sequence table
                first = false;
                System.out.println("CREATE TABLE sequence (\n"
                                   + "  id bigint(20) NOT NULL AUTO_INCREMENT,\n"
                                   + "  schema_name varchar(32) NOT NULL,\n"
                                   + "  table_name varchar(64) NOT NULL,\n"
                                   + "  begin_value bigint(20) NOT NULL,\n"
                                   + "  next_value bigint(20) NOT NULL,\n"
                                   + "  end_value bigint(20) DEFAULT NULL,\n"
                                   + "  step int(11),\n"
                                   + "  skip_n_steps int(11),\n"
                                   + "  select_order int(11) NOT NULL,\n"
                                   + "  version bigint(20) NOT NULL DEFAULT '0',\n"
                                   + "  deleted tinyint(1) NOT NULL DEFAULT '0',\n"
                                   + "  PRIMARY KEY (id),\n"
                                   + "  KEY idx_table_name_schema_name_deleted_select_order (table_name,schema_name,deleted,select_order) USING BTREE\n"
                                   + ") ENGINE=InnoDB DEFAULT CHARSET=utf8;");
                System.out.println("INSERT INTO sequence(id,schema_name,table_name,begin_value,next_value,end_value,step,skip_n_steps,select_order,version,deleted)"
                                   + "\n  VALUES(1, 'base', 'user', 0, 0, NULL, NULL, NULL, 0, 0, 0);");
            }
            for (ShardRouteInfo routeInfo : entry.getValue()) {
                System.out.println(String.format(createTableSql, routeInfo.getTbName()));// create db
            }
        }
    }

    private static ShardRouter buildShardRouter() {
        List<SimpleShardRouteRuleBinding> ruleBindings = new ArrayList<>();
        // define route rule
        SpelShardRouteRule idRouteRule = new SpelShardRouteRule("{scName}_{format('%02d', sdValue % 2)}",
                                                                "{tbName}_{format('%04d', sdValue % 8)}");

        // bind route rule to logical table
        SimpleShardRouteRuleBinding user = new SimpleShardRouteRuleBinding();
        user.setScName("base");
        user.setTbName("user");
        user.setSdKey("id");
        user.setSdValues("[0..7]");
        user.setRule(idRouteRule);
        ruleBindings.add(user);
        // TODO: build other tables to the rule
        // ...

        SimpleShardRouter shardRouter = new SimpleShardRouter(ruleBindings);
        SimpleShardParser shardParser = new SimpleShardParser(new JSQLParser(), shardRouter);
        return shardRouter;
    }

    private void rr() {
        // TODO: define raw datasource
        DataSource wds00 = null;
        DataSource rds00 = null;

        // 1. Define route rule
        SpelShardRouteRule routeRule = new SpelShardRouteRule("{scName}_{format('%02d', sdValue % 2)}",
                                                              "{tbName}_{format('%04d', sdValue % 8)}");

        // 2. Route rules bind to logical table name
        List<SimpleShardRouteRuleBinding> ruleBindings = new ArrayList<>();
        ruleBindings.add(new SimpleShardRouteRuleBinding("schema_name", "table_name", "id", routeRule, "[0..7]"));
        // TODO: add more route-rule-binding here ...
        SimpleShardRouter shardRouter = new SimpleShardRouter(ruleBindings);

        // 3. Schema names bind to datasource
        List<WeightedDataSource> weightedDataSources = new ArrayList<>();
        weightedDataSources.add(new WeightedDataSource(rds00, 9));
        weightedDataSources.add(new WeightedDataSource(wds00, 1));
        // TODO: add more datasource here ...
        List<ReadOnlyDataSourceBinding> readOnlyDataSourceBindings = new ArrayList<>();
        readOnlyDataSourceBindings.add(new ReadOnlyDataSourceBinding("schema_name_0[0..1]", weightedDataSources));
        List<WriteOnlyDataSourceBinding> writeOnlyDataSourceBindings = new ArrayList<>();
        writeOnlyDataSourceBindings.add(new WriteOnlyDataSourceBinding("schema_name_0[0..1]",
                                                                       (DataSource) Arrays.asList(wds00)));
        // TODO: add more datasource here ...
        DefaultReadWriteDataSourceManager dataSourceManager = new DefaultReadWriteDataSourceManager(
                                                                                                    readOnlyDataSourceBindings,
                                                                                                    null,
                                                                                                    writeOnlyDataSourceBindings,
                                                                                                    shardRouter);

        // 4. Define a 'DDRDataSource' to proxy the raw datasources
        DataSource ddrDataSource = new DefaultDDRDataSource(dataSourceManager, new SimpleShardParser(new JSQLParser(),
                                                                                                     shardRouter));
        // TODO: use 'ddrDataSource' to do your business here ...
    }
}
