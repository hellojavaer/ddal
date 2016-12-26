/*
 * Copyright 2016-2016 the original author or authors.
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
package org.hellojavaer.ddr.core.datasource.jdbc;

import org.hellojavaer.ddr.core.datasource.DataSourceSchemasBinding;
import org.hellojavaer.ddr.core.datasource.jdbc.init.UninitializedConnectionProcessor;
import org.hellojavaer.ddr.core.datasource.jdbc.init.UninitializedDataSourceProcessor;
import org.hellojavaer.ddr.core.datasource.jdbc.property.ConnectionProperty;
import org.hellojavaer.ddr.core.datasource.jdbc.property.DataSourceProperty;
import org.hellojavaer.ddr.core.datasource.manager.DataSourceManager;
import org.hellojavaer.ddr.core.datasource.manager.DataSourceParam;
import org.hellojavaer.ddr.core.sharding.ShardingParser;

import java.util.Map;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 10/12/2016.
 */
public class DefaultDDRDataSource extends AbstractDDRDataSource {

    private DataSourceManager           dataSourceManager;
    private ShardingParser              shardingParser;

    static {
        UninitializedDataSourceProcessor.setDefaultValue(DataSourceProperty.loginTimeout, 0, false);
        UninitializedConnectionProcessor.setDefaultValue(ConnectionProperty.autoCommit, true, true);
        UninitializedConnectionProcessor.setDefaultValue(ConnectionProperty.metaData, null, false);
    }

    public DataSourceManager getDataSourceManager() {
        return dataSourceManager;
    }

    public void setDataSourceManager(DataSourceManager dataSourceManager) {
        this.dataSourceManager = dataSourceManager;
    }

    public ShardingParser getShardingParser() {
        return shardingParser;
    }

    public void setShardingParser(ShardingParser shardingParser) {
        this.shardingParser = shardingParser;
    }

    public SQLParseResult parseSql(String sql, Map<Object, Object> jdbcParam) {
        return shardingParser.parse(sql, jdbcParam);
    }

    @Override
    public DataSourceSchemasBinding getDataSource(DataSourceParam param) {
        return dataSourceManager.getDataSource(param);
    }

}
