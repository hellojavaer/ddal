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
package org.hellojavaer.ddal.ddr.datasource.jdbc;

import org.hellojavaer.ddal.ddr.datasource.jdbc.init.UninitializedConnectionProcessor;
import org.hellojavaer.ddal.ddr.datasource.jdbc.init.UninitializedDataSourceProcessor;
import org.hellojavaer.ddal.ddr.datasource.jdbc.property.ConnectionProperty;
import org.hellojavaer.ddal.ddr.datasource.jdbc.property.DataSourceProperty;
import org.hellojavaer.ddal.ddr.datasource.manager.DataSourceManager;
import org.hellojavaer.ddal.ddr.datasource.manager.DataSourceParam;
import org.hellojavaer.ddal.ddr.shard.ShardParser;
import org.hellojavaer.ddal.ddr.sqlparse.SQLParsedResult;

import java.util.Collections;
import java.util.Map;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 10/12/2016.
 */
public class DefaultDDRDataSource extends AbstractDDRDataSource {

    private DataSourceManager dataSourceManager;
    private ShardParser       shardParser;

    static {
        UninitializedDataSourceProcessor.setDefaultValue(DataSourceProperty.loginTimeout, 0, false);
        UninitializedConnectionProcessor.setDefaultValue(ConnectionProperty.autoCommit, true, true);
        UninitializedConnectionProcessor.setDefaultValue(ConnectionProperty.metaData, null, false);
    }

    private DefaultDDRDataSource() {
    }

    public DefaultDDRDataSource(DataSourceManager dataSourceManager, ShardParser shardParser) {
        this.dataSourceManager = dataSourceManager;
        this.shardParser = shardParser;
    }

    public DataSourceManager getDataSourceManager() {
        return dataSourceManager;
    }

    public void setDataSourceManager(DataSourceManager dataSourceManager) {
        this.dataSourceManager = dataSourceManager;
    }

    public ShardParser getShardParser() {
        return shardParser;
    }

    public void setShardParser(ShardParser shardParser) {
        this.shardParser = shardParser;
    }

    @Override
    public SQLParsedResult parseSql(String sql, Map<Object, Object> jdbcParam) {
        if (shardParser == null) {
            SQLParsedResult sqlParsedResult = new SQLParsedResult();
            sqlParsedResult.setSql(sql);
            sqlParsedResult.setSchemas(Collections.<String> emptySet());
            return sqlParsedResult;
        } else {
            return shardParser.parse(sql, jdbcParam);
        }
    }

    @Override
    public DataSourceWrapper getDataSource(DataSourceParam param) {
        return dataSourceManager.getDataSource(param);
    }

}
