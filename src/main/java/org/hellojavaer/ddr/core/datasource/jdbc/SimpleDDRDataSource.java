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

import org.hellojavaer.ddr.core.datasource.manage.DataSourceManager;
import org.hellojavaer.ddr.core.datasource.manage.DataSourceParam;
import org.hellojavaer.ddr.core.datasource.tr.DistributedTransactionLevel;
import org.hellojavaer.ddr.core.datasource.tr.TransactionManagerAdapter;
import org.hellojavaer.ddr.core.sharding.ShardingParser;

import javax.sql.DataSource;
import java.util.Map;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">zoukaiming[邹凯明]</a>,created on 10/12/2016.
 */
public class SimpleDDRDataSource extends AbstractSimpleDDRDataSource {



    private DataSourceManager           dataSourceManager;
    private ShardingParser              shardingParser;
    private DistributedTransactionLevel distributedTransactionLevel;
    private TransactionManagerAdapter   transactionManagerAdapter;

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

    public DistributedTransactionLevel getDistributedTransactionLevel() {
        return distributedTransactionLevel;
    }

    public void setDistributedTransactionLevel(DistributedTransactionLevel distributedTransactionLevel) {
        this.distributedTransactionLevel = distributedTransactionLevel;
    }

    public TransactionManagerAdapter getTransactionManagerAdapter() {
        return transactionManagerAdapter;
    }

    public void setTransactionManagerAdapter(TransactionManagerAdapter transactionManagerAdapter) {
        this.transactionManagerAdapter = transactionManagerAdapter;
    }

    public String replaceSql(String sql, Map<Integer, Object> jdbcParam) {
        String tarSql = shardingParser.parse(sql, jdbcParam);
        return tarSql;
    }

    @Override
    public DataSource getDataSource(DataSourceParam param) {
        return dataSourceManager.getDataSource(param);
    }

}
