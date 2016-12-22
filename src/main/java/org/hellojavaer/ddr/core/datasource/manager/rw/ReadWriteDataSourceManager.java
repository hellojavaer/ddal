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
import org.hellojavaer.ddr.core.datasource.WeightedDataSource;
import org.hellojavaer.ddr.core.datasource.exception.CrossDataSourceException;
import org.hellojavaer.ddr.core.datasource.manager.DataSourceParam;
import org.hellojavaer.ddr.core.datasource.manager.rw.monitor.ReadOnlyDataSourceMonitor;
import org.hellojavaer.ddr.core.expression.range.RangeExpression;
import org.hellojavaer.ddr.core.expression.range.RangeItemVisitor;
import org.hellojavaer.ddr.core.lb.random.WeightItem;
import org.hellojavaer.ddr.core.lb.random.WeightedRandom;
import org.hellojavaer.ddr.core.utils.DDRJSONUtils;
import org.hellojavaer.ddr.core.utils.DDRStringUtils;

import javax.sql.DataSource;
import java.util.*;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 19/11/2016.
 */
public class ReadWriteDataSourceManager implements AbstractReadWriteDataSourceManager {

    // Original input
    private List<WriteOnlyDataSourceBinding>                       writeOnlyDataSources;
    private List<ReadOnlyDataSourceBinding>                        readOnlyDataSources;

    // cache
    private Map<String, WeightedRandom>                            readOnlyDataSourceQueryCache               = null;
    private Map<String, DataSourceSchemasBinding>                  writeOnlyDataSourceQueryCache              = null;

    private LinkedHashMap<String, List<WeightedDataSourceWrapper>> readOnlyDataSourceIndexCacheOriginalValues = null;
    private Map<String, Map<String, WeightedDataSourceWrapper>>    readOnlyDataSourceMapCahceOriginalValues   = null;

    private LinkedHashMap<String, List<WeightedDataSourceWrapper>> readOnlyDataSourceIndexCacheCurrentValues  = null;
    private Map<String, Map<String, WeightedDataSourceWrapper>>    readOnlyDataSourceMapCahceCurrentValues    = null;

    private class WeightedDataSourceWrapper extends WeightedDataSource {

        private Set<String> schemas;

        public WeightedDataSourceWrapper() {
        }

        public Set<String> getSchemas() {
            return schemas;
        }

        public void setSchemas(Set<String> schemas) {
            this.schemas = schemas;
        }
    }

    public synchronized ReadOnlyDataSourceMonitor getReadOnlyDataSourceMonitor() {

        return new ReadOnlyDataSourceMonitor() {

            @Override
            public Integer getWeight(String scName, int index) {
                scName = DDRStringUtils.toLowerCase(scName);
                if (scName == null || index < 0 || readOnlyDataSourceIndexCacheCurrentValues == null
                    || readOnlyDataSourceIndexCacheCurrentValues.isEmpty()) {
                    return null;
                }
                List<WeightedDataSourceWrapper> weightedDataSourceList = readOnlyDataSourceIndexCacheCurrentValues.get(scName);
                if (weightedDataSourceList == null || weightedDataSourceList.isEmpty()
                    || index >= weightedDataSourceList.size()) {
                    return null;
                }
                WeightedDataSource weightedDataSource = weightedDataSourceList.get(index);
                if (weightedDataSource == null) {
                    return null;
                }
                return weightedDataSource.getWeight();
            }

            @Override
            public String setWeight(String scName, int index, int weight) {
                scName = DDRStringUtils.toLowerCase(scName);
                if (scName == null || index < 0 || weight < 0) {
                    return new SetResult(SetResult.CODE_OF_PARAM_ERROR, "parameter invalid").toString();
                }
                if (readOnlyDataSourceIndexCacheCurrentValues == null
                    || readOnlyDataSourceIndexCacheCurrentValues.isEmpty()) {
                    return new SetResult(SetResult.CODE_OF_ILLEGAL_STATUS, "cache in empty").toString();
                }
                List<WeightedDataSourceWrapper> weightedDataSourceList = readOnlyDataSourceIndexCacheCurrentValues.get(scName);
                if (weightedDataSourceList == null || weightedDataSourceList.isEmpty()) {
                    return new SetResult(SetResult.CODE_OF_ILLEGAL_STATUS, "cache in empty").toString();
                }
                if (index < 0 || index >= weightedDataSourceList.size()) {
                    return new SetResult(SetResult.CODE_OF_PARAM_ERROR, "parameter invalid").toString();
                }
                WeightedDataSource weightedDataSource = weightedDataSourceList.get(index);
                if (weightedDataSource == null) {
                    return new SetResult(SetResult.CODE_OF_ILLEGAL_STATUS, "cache in empty").toString();
                }
                // ok
                weightedDataSource.setWeight(weight);
                refreshReadDataSourceQueryCache(scName, weightedDataSourceList);
                return new SetResult(SetResult.CODE_OF_SUCCESS, "OK").toString();
            }

            @Override
            public String restoreWeight(String scName, int index) {
                scName = DDRStringUtils.toLowerCase(scName);
                if (scName == null || index < 0) {
                    return new SetResult(SetResult.CODE_OF_PARAM_ERROR, "parameter invalid").toString();
                }
                if (readOnlyDataSourceIndexCacheOriginalValues == null
                    || readOnlyDataSourceIndexCacheOriginalValues.isEmpty()
                    || readOnlyDataSourceIndexCacheCurrentValues == null
                    || readOnlyDataSourceIndexCacheCurrentValues.isEmpty()) {
                    return new SetResult(SetResult.CODE_OF_ILLEGAL_STATUS, "cache in empty").toString();
                }
                List<WeightedDataSourceWrapper> weightedDataSourceList0 = readOnlyDataSourceIndexCacheOriginalValues.get(scName);
                List<WeightedDataSourceWrapper> weightedDataSourceList1 = readOnlyDataSourceIndexCacheCurrentValues.get(scName);
                if (weightedDataSourceList0 == null
                    || weightedDataSourceList0.isEmpty()
                    || weightedDataSourceList0.size() <= index//
                    || weightedDataSourceList1 == null || weightedDataSourceList1.isEmpty()
                    || weightedDataSourceList1.size() <= index) {
                    return new SetResult(SetResult.CODE_OF_ILLEGAL_STATUS, "cache in empty").toString();
                }
                WeightedDataSource weightedDataSource0 = weightedDataSourceList0.get(index);
                WeightedDataSource weightedDataSource1 = weightedDataSourceList0.get(index);
                if (weightedDataSource0 == null || weightedDataSource1 == null) {
                    return new SetResult(SetResult.CODE_OF_DATA_EXCEPTION, "cache in empty").toString();
                }
                // ok
                weightedDataSource1.setWeight(weightedDataSource0.getWeight());
                refreshReadDataSourceQueryCache(scName, weightedDataSourceList1);
                return new SetResult(SetResult.CODE_OF_SUCCESS, "OK").toString();
            }

            @Override
            public Integer getWeight(String scName, String dataSourceName) {
                scName = DDRStringUtils.toLowerCase(scName);
                dataSourceName = DDRStringUtils.toLowerCase(dataSourceName);
                if (scName == null || dataSourceName == null || readOnlyDataSourceMapCahceCurrentValues == null
                    || readOnlyDataSourceMapCahceCurrentValues.isEmpty()) {
                    return null;
                }
                Map<String, WeightedDataSourceWrapper> weightedDataSourceList = readOnlyDataSourceMapCahceCurrentValues.get(scName);
                if (weightedDataSourceList == null || weightedDataSourceList.isEmpty()) {
                    return null;
                }
                WeightedDataSource weightedDataSource = weightedDataSourceList.get(dataSourceName);
                if (weightedDataSource == null) {
                    return null;
                }
                return weightedDataSource.getWeight();
            }

            @Override
            public String setWeight(String scName, String dataSourceName, int weight) {
                scName = DDRStringUtils.toLowerCase(scName);
                dataSourceName = DDRStringUtils.toLowerCase(dataSourceName);
                if (scName == null || dataSourceName == null || weight < 0) {
                    return new SetResult(SetResult.CODE_OF_PARAM_ERROR, "parameter invalid").toString();
                }
                if (readOnlyDataSourceMapCahceCurrentValues == null
                    || readOnlyDataSourceMapCahceCurrentValues.isEmpty()) {
                    return new SetResult(SetResult.CODE_OF_ILLEGAL_STATUS, "cache in empty").toString();
                }
                Map<String, WeightedDataSourceWrapper> weightedDataSourceMap = readOnlyDataSourceMapCahceCurrentValues.get(scName);
                if (weightedDataSourceMap == null || weightedDataSourceMap.isEmpty()) {
                    return new SetResult(SetResult.CODE_OF_ILLEGAL_STATUS, "cache in empty").toString();
                }
                WeightedDataSource weightedDataSource = weightedDataSourceMap.get(dataSourceName);
                if (weightedDataSource == null) {
                    return new SetResult(SetResult.CODE_OF_ILLEGAL_STATUS, "cache in empty").toString();
                }
                // ok
                weightedDataSource.setWeight(weight);
                refreshReadDataSourceQueryCache(scName, readOnlyDataSourceIndexCacheCurrentValues.get(scName));
                return new SetResult(SetResult.CODE_OF_SUCCESS, "OK").toString();
            }

            @Override
            public String restoreWeight(String scName, String dataSourceName) {
                scName = DDRStringUtils.toLowerCase(scName);
                dataSourceName = DDRStringUtils.toLowerCase(dataSourceName);
                if (scName == null || dataSourceName == null) {
                    return new SetResult(SetResult.CODE_OF_PARAM_ERROR, "parameter invalid").toString();
                }
                if (readOnlyDataSourceMapCahceOriginalValues == null
                    || readOnlyDataSourceMapCahceOriginalValues.isEmpty()
                    || readOnlyDataSourceMapCahceCurrentValues == null
                    || readOnlyDataSourceMapCahceCurrentValues.isEmpty()) {
                    return new SetResult(SetResult.CODE_OF_ILLEGAL_STATUS, "cache in empty").toString();
                }
                Map<String, WeightedDataSourceWrapper> weightedDataSourceMap0 = readOnlyDataSourceMapCahceOriginalValues.get(scName);
                Map<String, WeightedDataSourceWrapper> weightedDataSourceMap1 = readOnlyDataSourceMapCahceCurrentValues.get(scName);
                if (weightedDataSourceMap0 == null || weightedDataSourceMap0.isEmpty()//
                    || weightedDataSourceMap1 == null || weightedDataSourceMap1.isEmpty()) {
                    return new SetResult(SetResult.CODE_OF_ILLEGAL_STATUS, "cache in empty").toString();
                }
                WeightedDataSource weightedDataSource0 = weightedDataSourceMap0.get(dataSourceName);
                WeightedDataSource weightedDataSource1 = weightedDataSourceMap1.get(dataSourceName);
                if (weightedDataSource0 == null || weightedDataSource1 == null) {
                    return new SetResult(SetResult.CODE_OF_DATA_EXCEPTION, "cache in empty").toString();
                }
                // ok
                weightedDataSource1.setWeight(weightedDataSource0.getWeight());
                refreshReadDataSourceQueryCache(scName, readOnlyDataSourceIndexCacheCurrentValues.get(scName));
                return new SetResult(SetResult.CODE_OF_SUCCESS, "OK").toString();
            }

            @Override
            public String getWeight(String scName) {
                scName = DDRStringUtils.toLowerCase(scName);
                if (readOnlyDataSourceIndexCacheOriginalValues == null
                    || readOnlyDataSourceIndexCacheOriginalValues.isEmpty()) {
                    return null;
                }
                return DDRJSONUtils.toJSONString(readOnlyDataSourceIndexCacheOriginalValues.get(scName));
            }

            @Override
            public String setWeight(String scName, String values) {// TODO
                return new SetResult(SetResult.CODE_OF_OPERATION_NOT_SUPPORTED, "").toString();
            }

            @Override
            public String restoreWeight(String scName) {
                scName = DDRStringUtils.toLowerCase(scName);
                if (scName == null) {
                    return new SetResult(SetResult.CODE_OF_PARAM_ERROR, "").toString();
                }
                if (readOnlyDataSourceIndexCacheOriginalValues == null
                    || readOnlyDataSourceIndexCacheOriginalValues.isEmpty()
                    || readOnlyDataSourceIndexCacheCurrentValues == null
                    || readOnlyDataSourceIndexCacheCurrentValues.isEmpty()) {
                    return new SetResult(SetResult.CODE_OF_ILLEGAL_STATUS, "").toString();
                }
                List<WeightedDataSourceWrapper> weightedDataSourceList0 = readOnlyDataSourceIndexCacheOriginalValues.get(scName);
                List<WeightedDataSourceWrapper> weightedDataSourceList1 = readOnlyDataSourceIndexCacheCurrentValues.get(scName);

                if (weightedDataSourceList0 == null || weightedDataSourceList0.isEmpty()
                    || weightedDataSourceList0 == null || weightedDataSourceList0.isEmpty()) {
                    return new SetResult(SetResult.CODE_OF_ILLEGAL_STATUS, "").toString();
                }
                for (int i = 0; i < weightedDataSourceList0.size(); i++) {
                    WeightedDataSource weightedDataSource0 = weightedDataSourceList0.get(i);
                    WeightedDataSource weightedDataSource1 = weightedDataSourceList1.get(i);
                    weightedDataSource1.setWeight(weightedDataSource0.getWeight());
                }
                // ok
                refreshReadDataSourceQueryCache(scName, weightedDataSourceList1);
                return new SetResult(SetResult.CODE_OF_SUCCESS, "").toString();
            }

            @Override
            public String restoreWeight() {
                if (readOnlyDataSourceIndexCacheOriginalValues == null
                    || readOnlyDataSourceIndexCacheOriginalValues.isEmpty()
                    || readOnlyDataSourceIndexCacheCurrentValues == null
                    || readOnlyDataSourceIndexCacheCurrentValues.isEmpty()) {
                    return new SetResult(SetResult.CODE_OF_ILLEGAL_STATUS, "").toString();
                }
                for (Map.Entry<String, List<WeightedDataSourceWrapper>> entry : readOnlyDataSourceIndexCacheOriginalValues.entrySet()) {
                    List<WeightedDataSourceWrapper> weightedDataSourceList0 = entry.getValue();
                    List<WeightedDataSourceWrapper> weightedDataSourceList1 = readOnlyDataSourceIndexCacheCurrentValues.get(entry.getKey());

                    if (weightedDataSourceList0 == null || weightedDataSourceList0.isEmpty()
                        || weightedDataSourceList0 == null || weightedDataSourceList0.isEmpty()) {
                        return new SetResult(SetResult.CODE_OF_ILLEGAL_STATUS, "").toString();
                    }
                    for (int i = 0; i < weightedDataSourceList0.size(); i++) {
                        WeightedDataSource weightedDataSource0 = weightedDataSourceList0.get(i);
                        WeightedDataSource weightedDataSource1 = weightedDataSourceList1.get(i);
                        weightedDataSource1.setWeight(weightedDataSource0.getWeight());
                    }
                }
                ReadWriteDataSourceManager.this.refreshReadDataSourceQueryCache();
                return new SetResult(SetResult.CODE_OF_SUCCESS, "").toString();
            }

            @Override
            public String getOriginalWeightConfig() {// {"scName:"[{"dataSourceName":"ds01",weight:2}]}
                return DDRJSONUtils.toJSONString(readOnlyDataSourceIndexCacheOriginalValues);
            }

            @Override
            public String getCurrentWeightConfig() {
                return DDRJSONUtils.toJSONString(readOnlyDataSourceIndexCacheCurrentValues);
            }

            private void refreshReadDataSourceQueryCache(String schema,
                                                         List<WeightedDataSourceWrapper> weightedDataSourceWrappers) {
                ReadWriteDataSourceManager.this.refreshReadDataSourceQueryCache(readOnlyDataSourceQueryCache, schema,
                                                                                weightedDataSourceWrappers);

            }
        };
    }

    public synchronized List<WriteOnlyDataSourceBinding> getWriteOnlyDataSources() {
        return writeOnlyDataSources;
    }

    public synchronized void setWriteOnlyDataSources(List<WriteOnlyDataSourceBinding> writeOnlyDataSources) {
        initWriteOnlyDataSource(writeOnlyDataSources);
        this.writeOnlyDataSources = writeOnlyDataSources;
    }

    public synchronized List<ReadOnlyDataSourceBinding> getReadOnlyDataSources() {
        return readOnlyDataSources;
    }

    public synchronized void setReadOnlyDataSources(List<ReadOnlyDataSourceBinding> readOnlyDataSources) {
        initReadOnlyDataSource(readOnlyDataSources);
        this.readOnlyDataSources = readOnlyDataSources;
    }

    private void initWriteOnlyDataSource(List<WriteOnlyDataSourceBinding> bindings) {
        if (bindings == null || bindings.isEmpty()) {
            throw new IllegalArgumentException("writeOnlyDataSourceQueryCache can't be empty");
        }
        final Map<String, DataSourceSchemasBinding> dataSourceMap = new HashMap<String, DataSourceSchemasBinding>();
        for (final WriteOnlyDataSourceBinding binding : bindings) {
            String schemasString = DDRStringUtils.trim(binding.getScNames());
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
        this.writeOnlyDataSourceQueryCache = dataSourceMap;
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
            schema = DDRStringUtils.trim(schema);
            if (schema == null) {
                throw new IllegalArgumentException("Schema of writeOnlyDataSources can't be null");
            }
            if (dataSource == null) {
                throw new IllegalArgumentException("[schema:" + schema
                                                   + "] dataSource of writeOnlyDataSources can't be null");
            }
            schema = DDRStringUtils.toLowerCase(schema);
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
        final LinkedHashMap<String, List<WeightedDataSourceWrapper>> readOnlyDataSourceIndexCacheOriginalValues = new LinkedHashMap<String, List<WeightedDataSourceWrapper>>();
        for (final ReadOnlyDataSourceBinding binding : bindings) {
            String schemasString = DDRStringUtils.trim(binding.getScNames());
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
            buildReadOnlyDataSource(readOnlyDataSourceIndexCacheOriginalValues, schemas, binding.getDataSources());
        }
        //
        this.readOnlyDataSourceIndexCacheOriginalValues = readOnlyDataSourceIndexCacheOriginalValues;
        this.readOnlyDataSourceIndexCacheCurrentValues = (LinkedHashMap<String, List<WeightedDataSourceWrapper>>) readOnlyDataSourceIndexCacheOriginalValues.clone();

        if (readOnlyDataSourceIndexCacheOriginalValues != null && !readOnlyDataSourceIndexCacheOriginalValues.isEmpty()) {
            HashMap<String, Map<String, WeightedDataSourceWrapper>> originalValues = new HashMap<String, Map<String, WeightedDataSourceWrapper>>();
            for (Map.Entry<String, List<WeightedDataSourceWrapper>> entry : readOnlyDataSourceIndexCacheOriginalValues.entrySet()) {
                if (entry.getValue() != null && !entry.getValue().isEmpty()) {
                    Map<String, WeightedDataSourceWrapper> weightedDataSourceWrapperMap = new HashMap<String, WeightedDataSourceWrapper>();
                    for (WeightedDataSourceWrapper weightedDataSourceWrapper : entry.getValue()) {
                        if (weightedDataSourceWrapper.getName() != null) {
                            weightedDataSourceWrapperMap.put(weightedDataSourceWrapper.getName(),
                                                             weightedDataSourceWrapper);
                        }
                    }
                    if (!weightedDataSourceWrapperMap.isEmpty()) {
                        originalValues.put(entry.getKey(), weightedDataSourceWrapperMap);
                    }
                }
            }
            this.readOnlyDataSourceMapCahceOriginalValues = originalValues;
            this.readOnlyDataSourceMapCahceCurrentValues = (Map<String, Map<String, WeightedDataSourceWrapper>>) originalValues.clone();
        } else {
            this.readOnlyDataSourceMapCahceOriginalValues = null;
            this.readOnlyDataSourceMapCahceCurrentValues = null;
        }
        refreshReadDataSourceQueryCache();
    }

    private void refreshReadDataSourceQueryCache() {
        if (readOnlyDataSourceIndexCacheOriginalValues == null || readOnlyDataSourceIndexCacheOriginalValues.isEmpty()) {
            this.readOnlyDataSourceQueryCache = null;
        } else {
            Map<String, WeightedRandom> map = new HashMap<>();
            for (Map.Entry<String, List<WeightedDataSourceWrapper>> entry : readOnlyDataSourceIndexCacheOriginalValues.entrySet()) {
                List<WeightedDataSourceWrapper> weightedDataSourceWrappers = entry.getValue();
                if (weightedDataSourceWrappers == null || weightedDataSourceWrappers.isEmpty()) {
                    continue;
                }
                refreshReadDataSourceQueryCache(map, entry.getKey(), entry.getValue());
            }
            this.readOnlyDataSourceQueryCache = map;
        }
    }

    private void refreshReadDataSourceQueryCache(Map<String, WeightedRandom> map, String schema,
                                                 List<WeightedDataSourceWrapper> weightedDataSourceWrappers) {
        List<WeightItem> dataSourceSchemasBindings = new ArrayList<WeightItem>();
        for (WeightedDataSourceWrapper weightedDataSourceWrapper : weightedDataSourceWrappers) {
            if (weightedDataSourceWrapper.getWeight() > 0) {
                DataSourceSchemasBinding dataSourceSchemasBinding = new DataSourceSchemasBinding(
                                                                                                 weightedDataSourceWrapper.getDataSource(),
                                                                                                 weightedDataSourceWrapper.getSchemas());
                WeightItem weightItem = new WeightItem(weightedDataSourceWrapper.getWeight(), dataSourceSchemasBinding);
                dataSourceSchemasBindings.add(weightItem);
            }
        }
        if (!dataSourceSchemasBindings.isEmpty()) {
            map.put(schema, new WeightedRandom(System.currentTimeMillis(), dataSourceSchemasBindings));
        } else {
            map.put(schema, null);
        }
    }

    private void buildReadOnlyDataSource(LinkedHashMap<String, List<WeightedDataSourceWrapper>> schemaDataSourceMapping,
                                         List<String> schemas, List<WeightedDataSource> dataSources) {
        Set<String> uniqSchemas = new HashSet<>();
        if (schemas != null && !schemas.isEmpty()) {
            for (String s : schemas) {
                s = DDRStringUtils.toLowerCase(s);
                if (s == null) {
                    throw new IllegalArgumentException("scName cann't be empty");
                } else if (uniqSchemas.contains(s)) {
                    throw new IllegalArgumentException("scName cann't repeat");
                } else {
                    uniqSchemas.add(s);
                }
            }
        }
        //
        List<WeightItem> itemList = new ArrayList<WeightItem>();
        List<WeightedDataSourceWrapper> dataSourceSchemasBindings = new ArrayList<WeightedDataSourceWrapper>();
        for (WeightedDataSource weightedDataSource : dataSources) {
            if (weightedDataSource.getWeight() == null || weightedDataSource.getWeight() < 0) {
                throw new IllegalArgumentException("weight can't be null or less than 0 for schemas '"
                                                   + DDRJSONUtils.toJSONString(uniqSchemas)
                                                   + "' of readOnlyDataSources");
            }
            if (weightedDataSource.getDataSource() == null) {
                throw new IllegalArgumentException("datasource can't be null for schemas '"
                                                   + DDRJSONUtils.toJSONString(uniqSchemas)
                                                   + "' of 'readOnlyDataSources'");
            }
            // 这里必须唯一索引
            WeightedDataSourceWrapper dataSourceSchemasBinding = new WeightedDataSourceWrapper();
            dataSourceSchemasBinding.setDataSource(weightedDataSource.getDataSource());
            dataSourceSchemasBinding.setName(DDRStringUtils.trim(weightedDataSource.getName()));
            dataSourceSchemasBinding.setDesc(weightedDataSource.getDesc());
            dataSourceSchemasBinding.setSchemas(uniqSchemas);
            dataSourceSchemasBinding.setWeight(weightedDataSource.getWeight());
            // add
            dataSourceSchemasBindings.add(dataSourceSchemasBinding);
            if (weightedDataSource.getWeight() == 0) {
                continue;
            } else {
                WeightItem weightItem = new WeightItem();
                weightItem.setWeight(weightedDataSource.getWeight());
                weightItem.setValue(dataSourceSchemasBinding);
                itemList.add(weightItem);
            }
        }
        //
        for (String schema : uniqSchemas) {
            if (schema == null) {
                throw new IllegalArgumentException("Schema of 'readOnlyDataSources' can't be null");
            }
            if (dataSources == null || dataSources.isEmpty()) {
                throw new IllegalArgumentException("[schema:" + schema
                                                   + "] dataSource of 'readOnlyDataSources' can't be empty");
            }
            schema = DDRStringUtils.toLowerCase(schema);
            if (schemaDataSourceMapping.containsKey(schema)) {
                throw new IllegalArgumentException("Schema '" + schema
                                                   + "' repeated bind in 'readOnlyDataSources' configuration");
            } else {
                schemaDataSourceMapping.put(schema, dataSourceSchemasBindings);
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
