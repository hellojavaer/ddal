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
package org.hellojavaer.ddr.datasource.ms;

import org.hellojavaer.ddr.datasource.DataSourceManager;
import org.hellojavaer.ddr.datasource.DataSourceManagerParam;
import org.hellojavaer.ddr.utils.StringUtils;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">zoukaiming[邹凯明]</a>,created on 19/11/2016.
 */
public class MasterSlaveDataSourceManager implements DataSourceManager {

    private List<MasterDataSourceBinding> masterDataSources;

    private List<SlaveDataSourceBinding>  slaveDataSources;

    private static final String           DEFAULT_SCHEMA        = "DEFAULT_SCHEMA";

    private Map<String, DataSource>       masterDataSourceCache = null;
    private Map<String, DataSource>       slaveDataSourceCache  = null;

    public List<MasterDataSourceBinding> getMasterDataSources() {
        return masterDataSources;
    }

    public void setMasterDataSources(List<MasterDataSourceBinding> masterDataSources) {
        this.masterDataSources = masterDataSources;
        initMaster(masterDataSources);
    }

    private void initMaster(List<MasterDataSourceBinding> bindings) {
        if (bindings == null || bindings.isEmpty()) {
            throw new IllegalArgumentException("masterDataSource can't be empty");
        }
        Map<String, DataSource> temp = new HashMap<String, DataSource>();
        for (MasterDataSourceBinding binding : bindings) {
            String schamesString = StringUtils.trim(binding.getScNames());
            if (schamesString == null) {
                schamesString = DEFAULT_SCHEMA;
            }
            String[] schemas = schamesString.split(",");
            for (String schema : schemas) {
                schema = StringUtils.trim(schema);
                if (schema != null) {
                    continue;
                }
                schema = schema.toLowerCase();
                DataSource ds = temp.get(schema);
                if (ds == null) {
                    DataSource bindingDataSource = binding.getDataSource();
                    if (bindingDataSource == null) {
                        throw new IllegalArgumentException("dataSource of masterDataSource can't be null");
                    } else {
                        temp.put(schema, bindingDataSource);
                    }
                } else {
                    throw new IllegalArgumentException("schema '" + schema
                                                       + "' bind repeated in masterDataSource configuration");
                }
            }
        }
        if (temp.isEmpty()) {
            throw new IllegalArgumentException("schames of masterDataSource can't be empty");
        }
        this.masterDataSourceCache = temp;
    }

    public List<SlaveDataSourceBinding> getSlaveDataSources() {
        return slaveDataSources;
    }

    public void setSlaveDataSources(List<SlaveDataSourceBinding> slaveDataSources) {
        this.slaveDataSources = slaveDataSources;
        initSlave(slaveDataSources);
    }

    private void initSlave(List<SlaveDataSourceBinding> bindings) {
        if (bindings == null || bindings.isEmpty()) {
            return;
        }
        Map<String, DataSource> temp = new HashMap<String, DataSource>();
        for (SlaveDataSourceBinding binding : bindings) {
            String schamesString = StringUtils.trim(binding.getScNames());
            if (schamesString == null) {
                schamesString = DEFAULT_SCHEMA;
            }
            String[] schemas = schamesString.split(",");
            for (String schema : schemas) {
                schema = StringUtils.trim(schema);
                if (schema != null) {
                    continue;
                }
                schema = schema.toLowerCase();
                DataSource ds = temp.get(schema);
                if (ds == null) {
                    List<DataSource> dataSources = binding.getDataSources();
                    if (dataSources == null || dataSources.isEmpty()) {
                        throw new IllegalArgumentException("dataSource of slaveDataSource can't be empty for schema '"
                                                           + schamesString + "'");
                    } else {
                        for (DataSource binddingDataSource : dataSources) {
                            temp.put(schema, binddingDataSource);
                        }
                    }
                } else {
                    throw new IllegalArgumentException("schema '" + schema
                                                       + "' bind repeated in slaveDataSource configuration");
                }
            }
        }
    }

    @Override
    public DataSource getDataSource(DataSourceManagerParam param) {
        String scName =  param.getScName();
        boolean readOnly =  param.isReadOnly();
        if (scName == null) {
            scName = DEFAULT_SCHEMA;
        }
        if (readOnly) {
            if (slaveDataSourceCache == null) {
                throw new IllegalStateException("no slaveDataSource is configed");
            } else {
                return slaveDataSourceCache.get(scName);
            }
        } else {
            if (masterDataSourceCache == null) {
                throw new IllegalStateException("no masterDataSource is configed");
            } else {
                return masterDataSourceCache.get(scName);
            }
        }
    }
}
