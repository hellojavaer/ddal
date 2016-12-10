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

    private static final String                   DEFAULT_SCHEMA                = "*";

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
            if (DEFAULT_SCHEMA.equals(schemasString)) {
                List<String> list = new ArrayList<String>();
                list.add(DEFAULT_SCHEMA);
                buildWriteOnlyDataSource(dataSourceMap, list, binding.getDataSource());
            } else {
                final List<String> schemas = new ArrayList<String>();
                RangeExpression.parse(schemasString, new RangeItemVisitor() {

                    @Override
                    public void visit(String val) {
                        schemas.add(val);
                    }
                });
                buildWriteOnlyDataSource(dataSourceMap, schemas, binding.getDataSource());
            }
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
                if (DEFAULT_SCHEMA.equals(s) && schemas.size() > 1) {
                    throw new IllegalArgumentException("default schema '*' can't be combined with other schema");
                }
                uniqSchemas.add(s);
            }
        }
        for (String schema : schemas) {
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
            if (DEFAULT_SCHEMA.equals(schemasString)) {
                List<String> list = new ArrayList<>();
                list.add(DEFAULT_SCHEMA);
                buildReadOnlyDataSource(dataSourceMap, list, binding.getDataSources());
            } else {
                final List<String> schemas = new ArrayList<String>();
                RangeExpression.parse(schemasString, new RangeItemVisitor() {

                    @Override
                    public void visit(String val) {
                        schemas.add(val);
                    }
                });
                buildReadOnlyDataSource(dataSourceMap, schemas, binding.getDataSources());
            }
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
                if (DEFAULT_SCHEMA.equals(s) && schemas.size() > 1) {
                    throw new IllegalArgumentException("default schema '*' can't be combined with other schema");
                }
                uniqSchemas.add(s);
            }
        }
        for (String schema : schemas) {
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
        String scNames = buildQueryKey(param.getScNames());
        boolean readOnly = param.isReadOnly();
        if (scNames == null) {
            scNames = DEFAULT_SCHEMA;
        }
        if (readOnly) {
            if (this.readOnlyDataSourceQueryCache == null) {
                throw new IllegalStateException("no readOnlyDataSource is configured");
            } else {
                WeightedRandom weightedRandom = this.readOnlyDataSourceQueryCache.get(scNames);
                if (weightedRandom == null) {
                    weightedRandom = this.readOnlyDataSourceQueryCache.get(DEFAULT_SCHEMA);
                }
                if (weightedRandom == null) {
                    throw new IllegalStateException("no readOnlyDataSource is configured for schemas:'" + scNames + "'");
                } else {
                    return (DataSourceSchemasBinding) weightedRandom.nextValue();
                }
            }
        } else {
            if (this.writeOnlyDataSourceQueryCache == null) {
                throw new IllegalStateException("no writeOnlyDataSource is configured");
            } else {
                DataSourceSchemasBinding dataSourceSchemasBinding = this.writeOnlyDataSourceQueryCache.get(scNames);
                if (dataSourceSchemasBinding == null) {
                    dataSourceSchemasBinding = this.writeOnlyDataSourceQueryCache.get(DEFAULT_SCHEMA);
                }
                if (dataSourceSchemasBinding == null) {
                    throw new IllegalStateException("no writeOnlyDataSource is configured for schemas:'" + scNames
                                                    + "'");
                } else {
                    return dataSourceSchemasBinding;
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
