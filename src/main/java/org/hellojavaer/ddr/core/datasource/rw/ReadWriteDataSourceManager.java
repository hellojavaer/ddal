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
package org.hellojavaer.ddr.core.datasource.rw;

import org.hellojavaer.ddr.core.datasource.DataSourceManager;
import org.hellojavaer.ddr.core.datasource.DataSourceManagerParam;
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

    private static final String              DEFAULT_SCHEMA           = "*";
    private List<WriteOnlyDataSourceBinding> writeOnlyDataSources;
    private List<ReadOnlyDataSourceBinding>  readOnlyDataSources;
    private Map<String, WeightedRandom>      readOnlyDataSourceCache  = null;
    private Map<String, DataSource>          writeOnlyDataSourceCache = null;

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
            throw new IllegalArgumentException("writeOnlyDataSourceCache can't be empty");
        }
        final Map<String, DataSource> dataSourceMap = new HashMap<String, DataSource>();
        for (final WriteOnlyDataSourceBinding binding : bindings) {
            String schemasString = StringUtils.trim(binding.getScNames());
            if (schemasString == null) {
                throw new IllegalArgumentException("scNames of writeOnlyDataSourceCache can't be empty");
            }
            if (DEFAULT_SCHEMA.equals(schemasString)) {
                buildWriteOnlyDataSource(dataSourceMap, DEFAULT_SCHEMA, binding.getDataSource());
            } else {
                RangeExpression.parse(schemasString, new RangeItemVisitor() {

                    @Override
                    public void visit(String val) {
                        buildWriteOnlyDataSource(dataSourceMap, val, binding.getDataSource());
                    }
                });
            }
        }
        if (dataSourceMap.isEmpty()) {
            // TODO log.warning
        } else {
            this.writeOnlyDataSourceCache = dataSourceMap;
        }
    }

    private void buildWriteOnlyDataSource(Map<String, DataSource> dataSourceMap, String schema, DataSource dataSource) {
        schema = StringUtils.trim(schema);
        if (schema == null) {
            throw new IllegalArgumentException("schema of writeOnlyDataSources can't be null");
        }
        if (dataSource == null) {
            throw new IllegalArgumentException("[schema:" + schema
                                               + "] dataSource of writeOnlyDataSources can't be null");
        }
        schema = schema.toLowerCase();
        boolean exist = dataSourceMap.containsKey(schema);
        if (exist) {
            throw new IllegalArgumentException("schema '" + schema
                                               + "' bind repeated in writeOnlyDataSources configuration");
        } else {
            dataSourceMap.put(schema, dataSource);
        }
    }

    private void initReadOnlyDataSource(List<ReadOnlyDataSourceBinding> bindings) {
        if (bindings == null || bindings.isEmpty()) {
            throw new IllegalArgumentException("readOnlyDataSourceCache can't be empty");
        }
        final Map<String, WeightedRandom> dataSourceMap = new HashMap<String, WeightedRandom>();
        for (final ReadOnlyDataSourceBinding binding : bindings) {
            String schemasString = StringUtils.trim(binding.getScNames());
            if (schemasString == null) {
                throw new IllegalArgumentException("scNames of readOnlyDataSourceCache can't be empty");
            }
            if (DEFAULT_SCHEMA.equals(schemasString)) {
                buildReadOnlyDataSource(dataSourceMap, DEFAULT_SCHEMA, binding.getDataSources());
            } else {
                RangeExpression.parse(schemasString, new RangeItemVisitor() {

                    @Override
                    public void visit(String val) {
                        buildReadOnlyDataSource(dataSourceMap, val, binding.getDataSources());
                    }
                });
            }
        }
        if (dataSourceMap.isEmpty()) {
            // TODO log.warning
        } else {
            this.readOnlyDataSourceCache = dataSourceMap;
        }
    }

    private void buildReadOnlyDataSource(Map<String, WeightedRandom> dataSourceMap, String schema,
                                         List<WeightedDataSource> dataSources) {
        schema = StringUtils.trim(schema);
        if (schema == null) {
            throw new IllegalArgumentException("schema of readOnlyDataSources can't be null");
        }
        if (dataSources == null || dataSources.isEmpty()) {
            throw new IllegalArgumentException("[schema:" + schema
                                               + "] dataSource of readOnlyDataSources can't be empty");
        }
        schema = schema.toLowerCase();
        boolean exist = dataSourceMap.containsKey(schema);
        if (exist) {
            throw new IllegalArgumentException("schema '" + schema
                                               + "' bind repeated in readOnlyDataSources configuration");
        } else {
            List<WeightItem> itemList = new ArrayList<WeightItem>();
            for (WeightedDataSource weightedDataSource : dataSources) {
                if (weightedDataSource.getWeight() == null) {
                    throw new IllegalArgumentException("weight can't be null for schema '" + schema + "' of readOnlyDataSources");
                }
                if (weightedDataSource.getDataSource() == null) {
                    throw new IllegalArgumentException("datasource can't be null for schema '" + schema + "' of readOnlyDataSources");
                }
                WeightItem weightItem = new WeightItem();
                weightItem.setWeight(weightedDataSource.getWeight());
                weightItem.setValue(weightedDataSource.getDataSource());
                itemList.add(weightItem);
            }
            WeightedRandom weightedRandom = new WeightedRandom(System.currentTimeMillis(), itemList);
            dataSourceMap.put(schema, weightedRandom);
        }
    }

    @Override
    public DataSource getDataSource(DataSourceManagerParam param) {
        String scNames = buildQueryKey(param.getScNames());
        boolean readOnly = param.isReadOnly();
        if (scNames == null) {
            scNames = DEFAULT_SCHEMA;
        }
        if (readOnly) {
            if (this.readOnlyDataSourceCache == null) {
                throw new IllegalStateException("no readOnlyDataSource is configured");
            } else {
                WeightedRandom weightedRandom = this.readOnlyDataSourceCache.get(scNames);
                if (weightedRandom == null) {
                    weightedRandom = this.readOnlyDataSourceCache.get(DEFAULT_SCHEMA);
                }
                if (weightedRandom == null) {
                    throw new IllegalStateException("no readOnlyDataSource is configured for schemas:'" + scNames
                                                    + "'");
                } else {
                    return (DataSource) weightedRandom.nextValue();
                }
            }
        } else {
            if (this.writeOnlyDataSourceCache == null) {
                throw new IllegalStateException("no writeOnlyDataSource is configured");
            } else {
                DataSource dataSource = this.writeOnlyDataSourceCache.get(scNames);
                if (dataSource == null) {
                    dataSource = this.writeOnlyDataSourceCache.get(DEFAULT_SCHEMA);
                }
                if (dataSource == null) {
                    throw new IllegalStateException("no writeOnlyDataSource is configured for schemas:'" + scNames
                                                    + "'");
                } else {
                    return dataSource;
                }
            }
        }
    }

    private String buildQueryKey(Set<String> scNames) {
        if (scNames == null || scNames.isEmpty()) {
            return "null;";
        } else {
            String[] scNameArray = new String[scNames.size()];
            int i = 0;
            for (String s : scNames) {
                if (s == null) {
                    scNameArray[i++] = "null";
                } else {
                    scNameArray[i++] = s.trim().toLowerCase();
                }
            }
            Arrays.sort(scNames.toArray(scNameArray));
            StringBuilder sb = new StringBuilder();
            for (String s : scNameArray) {
                sb.append(s);
                sb.append(';');
            }
            return sb.toString();
        }
    }
}
