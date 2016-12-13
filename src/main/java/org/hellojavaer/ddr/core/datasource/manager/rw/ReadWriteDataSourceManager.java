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
package org.hellojavaer.ddr.core.datasource.manager.rw;

import org.hellojavaer.ddr.core.datasource.DataSourceSchemasBinding;
import org.hellojavaer.ddr.core.datasource.manager.DataSourceManager;
import org.hellojavaer.ddr.core.datasource.manager.DataSourceParam;
import org.hellojavaer.ddr.core.datasource.WeightedDataSource;
import org.hellojavaer.ddr.core.datasource.exception.CrossDataSourceException;
import org.hellojavaer.ddr.core.expression.range.RangeExpression;
import org.hellojavaer.ddr.core.expression.range.RangeItemVisitor;
import org.hellojavaer.ddr.core.strategy.WeightItem;
import org.hellojavaer.ddr.core.strategy.WeightedRandom;
import org.hellojavaer.ddr.core.utils.StringUtils;

import javax.sql.DataSource;
import java.util.*;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">zoukaiming[邹凯明]</a>,created on 19/11/2016.
 */
public class ReadWriteDataSourceManager implements DataSourceManager {

    private List<WriteOnlyDataSourceBinding>      writeOnlyDataSources;
    private List<ReadOnlyDataSourceBinding>       readOnlyDataSources;

    private Map<String, WeightedRandom>           readOnlyDataSourceQueryCache  = null;
    private Map<String, DataSourceSchemasBinding> writeOnlyDataSourceQueryCache = null;

    public List<WriteOnlyDataSourceBinding> getWriteOnlyDataSources() {
        return writeOnlyDataSources;
    }

    public void setWriteOnlyDataSources(List<WriteOnlyDataSourceBinding> writeOnlyDataSources) {
        initWriteOnlyDataSource(writeOnlyDataSources);
        this.writeOnlyDataSources = writeOnlyDataSources;
    }

    public List<ReadOnlyDataSourceBinding> getReadOnlyDataSources() {
        return readOnlyDataSources;
    }

    public void setReadOnlyDataSources(List<ReadOnlyDataSourceBinding> readOnlyDataSources) {
        initReadOnlyDataSource(readOnlyDataSources);
        this.readOnlyDataSources = readOnlyDataSources;
    }

    private void initWriteOnlyDataSource(List<WriteOnlyDataSourceBinding> bindings) {
        if (bindings == null || bindings.isEmpty()) {
            throw new IllegalArgumentException("writeOnlyDataSourceQueryCache can't be empty");
        }
        final Map<String, DataSourceSchemasBinding> dataSourceMap = new HashMap<String, DataSourceSchemasBinding>();
        for (final WriteOnlyDataSourceBinding binding : bindings) {
            String schemasString = StringUtils.trim(binding.getScNames());
            if (schemasString == null) {
                throw new IllegalArgumentException("scNames of writeOnlyDataSourceQueryCache can't be empty");
            }
            final List<String> schemas = new ArrayList<String>();
            RangeExpression.parse(schemasString, new RangeItemVisitor() {

                @Override
                public void visit(String val) {
                    schemas.add(val);
                }
            });
            buildWriteOnlyDataSource(dataSourceMap, schemas, binding.getDataSource());

        }
        if (dataSourceMap.isEmpty()) {
            // TODO log.warning
        } else {
            this.writeOnlyDataSourceQueryCache = dataSourceMap;
        }
    }

    private void buildWriteOnlyDataSource(Map<String, DataSourceSchemasBinding> dataSourceMap, List<String> schemas,
                                          DataSource dataSource) {
        Set<String> uniqSchemas = new HashSet<>();
        if (schemas != null && !schemas.isEmpty()) {
            for (String s : schemas) {
                uniqSchemas.add(s);
            }
        }
        for (String schema : schemas) {
            schema = StringUtils.trim(schema);
            if (schema == null) {
                throw new IllegalArgumentException("Schema of writeOnlyDataSources can't be null");
            }
            if (dataSource == null) {
                throw new IllegalArgumentException("[schema:" + schema
                                                   + "] dataSource of writeOnlyDataSources can't be null");
            }
            schema = schema.toLowerCase();
            boolean exist = dataSourceMap.containsKey(schema);
            if (exist) {
                throw new IllegalArgumentException("Schema '" + schema
                                                   + "' repeated binding in writeOnlyDataSources configuration");
            } else {
                dataSourceMap.put(schema, new DataSourceSchemasBinding(dataSource, uniqSchemas));
            }
        }
    }

    private void initReadOnlyDataSource(List<ReadOnlyDataSourceBinding> bindings) {
        if (bindings == null || bindings.isEmpty()) {
            throw new IllegalArgumentException("readOnlyDataSourceQueryCache can't be empty");
        }
        final Map<String, WeightedRandom> dataSourceMap = new HashMap<String, WeightedRandom>();
        for (final ReadOnlyDataSourceBinding binding : bindings) {
            String schemasString = StringUtils.trim(binding.getScNames());
            if (schemasString == null) {
                throw new IllegalArgumentException("scNames of readOnlyDataSourceQueryCache can't be empty");
            }
            final List<String> schemas = new ArrayList<String>();
            RangeExpression.parse(schemasString, new RangeItemVisitor() {

                @Override
                public void visit(String val) {
                    schemas.add(val);
                }
            });
            buildReadOnlyDataSource(dataSourceMap, schemas, binding.getDataSources());
        }
        if (dataSourceMap.isEmpty()) {
            // TODO log.warning
        } else {
            this.readOnlyDataSourceQueryCache = dataSourceMap;
        }
    }

    private void buildReadOnlyDataSource(Map<String, WeightedRandom> dataSourceMap, List<String> schemas,
                                         List<WeightedDataSource> dataSources) {
        Set<String> uniqSchemas = new HashSet<>();
        if (schemas != null && !schemas.isEmpty()) {
            for (String s : schemas) {
                uniqSchemas.add(s);
            }
        }
        for (String schema : schemas) {
            schema = StringUtils.trim(schema);
            if (schema == null) {
                throw new IllegalArgumentException("Schema of readOnlyDataSources can't be null");
            }
            if (dataSources == null || dataSources.isEmpty()) {
                throw new IllegalArgumentException("[schema:" + schema
                                                   + "] dataSource of readOnlyDataSources can't be empty");
            }
            schema = schema.toLowerCase();
            boolean exist = dataSourceMap.containsKey(schema);
            if (exist) {
                throw new IllegalArgumentException("Schema '" + schema
                                                   + "' repeated binding in readOnlyDataSources configuration");
            } else {
                List<WeightItem> itemList = new ArrayList<WeightItem>();
                for (WeightedDataSource weightedDataSource : dataSources) {
                    if (weightedDataSource.getWeight() == null) {
                        throw new IllegalArgumentException("weight can't be null for schema '" + schema
                                                           + "' of readOnlyDataSources");
                    }
                    if (weightedDataSource.getDataSource() == null) {
                        throw new IllegalArgumentException("datasource can't be null for schema '" + schema
                                                           + "' of readOnlyDataSources");
                    }
                    WeightItem weightItem = new WeightItem();
                    weightItem.setWeight(weightedDataSource.getWeight());
                    weightItem.setValue(new DataSourceSchemasBinding(weightedDataSource.getDataSource(), uniqSchemas));
                    itemList.add(weightItem);
                }
                WeightedRandom weightedRandom = new WeightedRandom(System.currentTimeMillis(), itemList);
                dataSourceMap.put(schema, weightedRandom);
            }
        }
    }

    @Override
    public DataSourceSchemasBinding getDataSource(DataSourceParam param) {
        if (param.getScNames() == null || param.getScNames().isEmpty()) {
            throw new IllegalArgumentException("scNames can't be empty");
        }
        boolean readOnly = param.isReadOnly();
        if (readOnly) {
            if (this.readOnlyDataSourceQueryCache == null) {
                throw new IllegalStateException("No readOnlyDataSource is configured");
            } else {
                DataSourceSchemasBinding dataSourceSchemasBinding = null;
                for (String scName : param.getScNames()) {
                    if (dataSourceSchemasBinding == null) {
                        WeightedRandom weightedRandom = this.readOnlyDataSourceQueryCache.get(scName);
                        if (weightedRandom == null) {
                            throw new IllegalStateException("No readOnlyDataSource is configured for schema:'" + scName
                                                            + "'");
                        } else {
                            dataSourceSchemasBinding = (DataSourceSchemasBinding) weightedRandom.nextValue();
                        }
                    } else {
                        if (!dataSourceSchemasBinding.getSchemas().contains(scName)) {
                            throw new CrossDataSourceException("scName:'" + scName
                                                               + "' is not in readOnlyDataSource binding '"
                                                               + dataSourceSchemasBinding.toString() + "'");
                        }
                    }
                }
                return dataSourceSchemasBinding;
            }
        } else {
            if (this.writeOnlyDataSourceQueryCache == null) {
                throw new IllegalStateException("No writeOnlyDataSource is configured");
            } else {
                DataSourceSchemasBinding dataSourceSchemasBinding = null;
                for (String scName : param.getScNames()) {
                    if (dataSourceSchemasBinding == null) {
                        dataSourceSchemasBinding = this.writeOnlyDataSourceQueryCache.get(scName);
                        if (dataSourceSchemasBinding == null) {
                            throw new IllegalStateException("No writeOnlyDataSource is configured for schema:'"
                                                            + scName + "'");
                        }
                    } else {
                        if (!dataSourceSchemasBinding.getSchemas().contains(scName)) {
                            throw new CrossDataSourceException("scName:'" + scName
                                                               + "' is not in writeOnlyDataSource binding '"
                                                               + dataSourceSchemasBinding.toString() + "'");
                        }
                    }
                }
                return dataSourceSchemasBinding;
            }
        }
    }

}
