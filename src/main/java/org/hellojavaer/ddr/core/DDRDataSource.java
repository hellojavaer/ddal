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
package org.hellojavaer.ddr.core;

import org.hellojavaer.ddr.core.datasource.AbstarctDDRDateSource;
import org.hellojavaer.ddr.core.datasource.DataSourceManager;
import org.hellojavaer.ddr.core.datasource.DataSourceManagerParam;
import org.hellojavaer.ddr.core.datasource.SingleDataSourceManager;
import org.hellojavaer.ddr.core.sharding.ShardingRouteParser;

import javax.sql.DataSource;
import java.lang.reflect.Method;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">zoukaiming[邹凯明]</a>,created on 05/11/2016.
 */
public class DDRDataSource extends AbstarctDDRDateSource {

    private DataSourceManager   dataSourceManager;
    private ShardingRouteParser shardingRouteParser;

    public DataSourceManager getDataSourceManager() {
        return dataSourceManager;
    }

    public void setDataSourceManager(DataSourceManager dataSourceManager) {
        this.dataSourceManager = dataSourceManager;
    }

    public ShardingRouteParser getShardingRouteParser() {
        return shardingRouteParser;
    }

    public void setShardingRouteParser(ShardingRouteParser shardingRouteParser) {
        this.shardingRouteParser = shardingRouteParser;
    }

    public String replaceSql(String sql) {
        String tarSql = shardingRouteParser.parse(sql);
        return tarSql;
    }

    @Override
    protected DataSource getDataSource() {
        DataSourceManagerParam param = new DataSourceManagerParam();
        if (dataSourceManager instanceof SingleDataSourceManager) {
            param.setReadOnly(false);
        } else {
            param.setReadOnly(isReadOnly());// FIXME
        }
        return dataSourceManager.getDataSource(param);
    }

    private boolean isReadOnly() {
        try {
            Boolean readOnly = (Boolean) getTransactionSynchronizationManagerMethod().invoke(null, null);
            return readOnly;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // spring
    private Method isCurrentTransactionReadOnly = null;

    private Method getTransactionSynchronizationManagerMethod() {
        if (isCurrentTransactionReadOnly == null) {
            synchronized (this) {
                if (isCurrentTransactionReadOnly == null) {
                    Method method = null;
                    try {
                        Class clazz = Class.forName("org.springframework.transaction.support.TransactionSynchronizationManager");
                        if (clazz == null) {
                            throw new IllegalStateException(
                                                            "masterSlaveDataSourceManager now dependency spring TransactionSynchronizationManager");
                        }
                        method = clazz.getMethod("isCurrentTransactionReadOnly");
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    isCurrentTransactionReadOnly = method;
                }
            }
        }
        return isCurrentTransactionReadOnly;
    }

}
