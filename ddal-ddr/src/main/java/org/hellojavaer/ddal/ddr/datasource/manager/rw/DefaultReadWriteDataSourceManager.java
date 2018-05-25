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
package org.hellojavaer.ddal.ddr.datasource.manager.rw;

import org.hellojavaer.ddal.ddr.datasource.WeightedDataSource;
import org.hellojavaer.ddal.ddr.datasource.exception.CrossDataSourceException;
import org.hellojavaer.ddal.ddr.datasource.exception.DataSourceNotFoundException;
import org.hellojavaer.ddal.ddr.datasource.jdbc.DataSourceWrapper;
import org.hellojavaer.ddal.ddr.datasource.manager.DataSourceParam;
import org.hellojavaer.ddal.ddr.datasource.manager.rw.monitor.ReadOnlyDataSourceMonitor;
import org.hellojavaer.ddal.ddr.datasource.manager.rw.monitor.ReadOnlyDataSourceMonitorServer;
import org.hellojavaer.ddal.ddr.datasource.manager.rw.monitor.WriterMethodInvokeResult;
import org.hellojavaer.ddal.ddr.datasource.security.metadata.DefaultMetaDataChecker;
import org.hellojavaer.ddal.ddr.datasource.security.metadata.MetaDataChecker;
import org.hellojavaer.ddal.ddr.expression.range.RangeExpressionItemVisitor;
import org.hellojavaer.ddal.ddr.expression.range.RangeExpressionParser;
import org.hellojavaer.ddal.ddr.lb.random.WeightItem;
import org.hellojavaer.ddal.ddr.lb.random.WeightedRandom;
import org.hellojavaer.ddal.ddr.shard.ShardRouteInfo;
import org.hellojavaer.ddal.ddr.shard.ShardRouter;
import org.hellojavaer.ddal.ddr.utils.DDRJSONUtils;
import org.hellojavaer.ddal.ddr.utils.DDRStringUtils;
import org.hellojavaer.ddal.ddr.utils.DDRToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 19/11/2016.
 */
public class DefaultReadWriteDataSourceManager implements ReadWriteDataSourceManager {

    protected final Logger                                         stdLogger                                  = LoggerFactory.getLogger("org.hellojavaer.ddr.ds");

    // Original input
    private ReadOnlyDataSourceMonitorServer                        readOnlyDataSourceMonitorServer;

    // Original input
    private List<WriteOnlyDataSourceBinding>                       writeOnlyDataSources                       = null;
    private List<ReadOnlyDataSourceBinding>                        readOnlyDataSources                        = null;

    // Original input
    private ShardRouter                                            shardRouter                                = null;
    private MetaDataChecker                                        metaDataChecker                            = null;

    // cache
    private Map<String, WeightedRandom>                            readOnlyDataSourceQueryCache               = null;
    private Map<String, DataSourceWrapper>                         writeOnlyDataSourceQueryCache              = null;

    // backup {physical schema name <-> datasources}
    private LinkedHashMap<String, List<WeightedDataSourceWrapper>> readOnlyDataSourceIndexCacheOriginalValues = null;
    private Map<String, Map<String, WeightedDataSourceWrapper>>    readOnlyDataSourceMapCacheOriginalValues   = null;

    private LinkedHashMap<String, List<WeightedDataSourceWrapper>> readOnlyDataSourceIndexCacheCurrentValues  = null;
    private Map<String, Map<String, WeightedDataSourceWrapper>>    readOnlyDataSourceMapCahceCurrentValues    = null;

    // tag
    private boolean                                                initialized                                = false;

    // cache
    private volatile Map<String, Set<String>>                      physicalTables                             = null;

    private DefaultReadWriteDataSourceManager() {
        this.metaDataChecker = new DefaultMetaDataChecker("mysql");
    }

    public DefaultReadWriteDataSourceManager(List<ReadOnlyDataSourceBinding> readOnlyDataSources,
                                             ReadOnlyDataSourceMonitorServer readOnlyDataSourceMonitorServer,
                                             List<WriteOnlyDataSourceBinding> writeOnlyDataSources,
                                             ShardRouter shardRouter) {
        setReadOnlyDataSources(readOnlyDataSources);
        setReadOnlyDataSourceMonitorServer(readOnlyDataSourceMonitorServer);
        setWriteOnlyDataSources(writeOnlyDataSources);
        setShardRouter(shardRouter);
        setMetaDataChecker(new DefaultMetaDataChecker("mysql"));
        init();
    }

    public DefaultReadWriteDataSourceManager(ReadOnlyDataSourceMonitorServer readOnlyDataSourceMonitorServer,
                                             List<WriteOnlyDataSourceBinding> writeOnlyDataSources,
                                             List<ReadOnlyDataSourceBinding> readOnlyDataSources,
                                             ShardRouter shardRouter, MetaDataChecker metaDataChecker) {
        setReadOnlyDataSources(readOnlyDataSources);
        setReadOnlyDataSourceMonitorServer(readOnlyDataSourceMonitorServer);
        setWriteOnlyDataSources(writeOnlyDataSources);
        setShardRouter(shardRouter);
        setMetaDataChecker(metaDataChecker);
        init();
    }

    public synchronized List<ReadOnlyDataSourceBinding> getReadOnlyDataSources() {
        return readOnlyDataSources;
    }

    public synchronized void setReadOnlyDataSources(List<ReadOnlyDataSourceBinding> readOnlyDataSources) {
        initReadOnlyDataSource(readOnlyDataSources);
        check(readOnlyDataSourceIndexCacheOriginalValues);
        this.readOnlyDataSources = readOnlyDataSources;
    }

    @Override
    public ReadOnlyDataSourceMonitorServer getReadOnlyDataSourceMonitorServer() {
        return this.readOnlyDataSourceMonitorServer;
    }

    public void setReadOnlyDataSourceMonitorServer(ReadOnlyDataSourceMonitorServer readOnlyDataSourceMonitorServer) {
        this.readOnlyDataSourceMonitorServer = readOnlyDataSourceMonitorServer;
    }

    public synchronized List<WriteOnlyDataSourceBinding> getWriteOnlyDataSources() {
        return writeOnlyDataSources;
    }

    public synchronized void setWriteOnlyDataSources(List<WriteOnlyDataSourceBinding> writeOnlyDataSources) {
        initWriteOnlyDataSource(writeOnlyDataSources);
        check(writeOnlyDataSourceQueryCache);
        this.writeOnlyDataSources = writeOnlyDataSources;
    }

    public ShardRouter getShardRouter() {
        return shardRouter;
    }

    public void setShardRouter(ShardRouter shardRouter) {
        this.shardRouter = shardRouter;
    }

    public MetaDataChecker getMetaDataChecker() {
        return metaDataChecker;
    }

    public void setMetaDataChecker(MetaDataChecker metaDataChecker) {
        this.metaDataChecker = metaDataChecker;
    }

    private void init() {
        if (initialized == false && readOnlyDataSourceMonitorServer != null) {
            synchronized (this) {
                if (initialized == false && readOnlyDataSourceMonitorServer != null) {
                    readOnlyDataSourceMonitorServer.init(getReadOnlyDataSourceMonitor());
                    initialized = true;
                }
            }
        }
    }

    private static class WeightedDataSourceWrapper extends WeightedDataSource implements Cloneable {

        private int               index;
        private DataSourceWrapper dataSourceWrapper;

        public WeightedDataSourceWrapper() {
        }

        public int getIndex() {
            return index;
        }

        public void setIndex(int index) {
            this.index = index;
        }

        public DataSourceWrapper getDataSourceWrapper() {
            return dataSourceWrapper;
        }

        public void setDataSourceWrapper(DataSourceWrapper dataSourceWrapper) {
            this.dataSourceWrapper = dataSourceWrapper;
        }

        @Override
        public WeightedDataSourceWrapper clone() {
            try {
                WeightedDataSourceWrapper backup = (WeightedDataSourceWrapper) super.clone();
                backup.setIndex(index);
                backup.setDataSourceWrapper(dataSourceWrapper);
                return backup;
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String toString() {
            return new DDRToStringBuilder()//
            .append("name", getName())//
            .append("index", index)//
            .append("weight", getWeight())//
            .append("desc", getDesc())//
            .append("schemas", dataSourceWrapper.getSchemas())//
            .append("datasource", getDataSource())//
            .toString();
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
                    return new WriterMethodInvokeResult(WriterMethodInvokeResult.CODE_OF_ILLEGAL_ARGUMENT,
                                                        "parameter invalid").toString();
                }
                if (readOnlyDataSourceIndexCacheCurrentValues == null
                    || readOnlyDataSourceIndexCacheCurrentValues.isEmpty()) {
                    return new WriterMethodInvokeResult(WriterMethodInvokeResult.CODE_OF_DATA_IS_EMPTY,
                                                        "target data is empty").toString();
                }
                List<WeightedDataSourceWrapper> weightedDataSourceList = readOnlyDataSourceIndexCacheCurrentValues.get(scName);
                if (weightedDataSourceList == null || weightedDataSourceList.isEmpty()) {
                    return new WriterMethodInvokeResult(WriterMethodInvokeResult.CODE_OF_DATA_IS_EMPTY,
                                                        "target data is empty").toString();
                }
                if (index < 0 || index >= weightedDataSourceList.size()) {
                    return new WriterMethodInvokeResult(WriterMethodInvokeResult.CODE_OF_ILLEGAL_ARGUMENT,
                                                        "illegal argument(s)").toString();
                }
                WeightedDataSource weightedDataSource = weightedDataSourceList.get(index);
                if (weightedDataSource == null) {
                    return new WriterMethodInvokeResult(WriterMethodInvokeResult.CODE_OF_DATA_IS_EMPTY,
                                                        "target data is empty").toString();
                }
                // ok
                weightedDataSource.setWeight(weight);
                refreshReadDataSourceQueryCache(scName, weightedDataSourceList);
                return new WriterMethodInvokeResult(WriterMethodInvokeResult.CODE_OF_SUCCESS, "OK").toString();
            }

            @Override
            public String restoreWeight(String scName, int index) {
                scName = DDRStringUtils.toLowerCase(scName);
                if (scName == null || index < 0) {
                    return new WriterMethodInvokeResult(WriterMethodInvokeResult.CODE_OF_ILLEGAL_ARGUMENT,
                                                        "illegal argument(s)").toString();
                }
                if (readOnlyDataSourceIndexCacheOriginalValues == null
                    || readOnlyDataSourceIndexCacheOriginalValues.isEmpty()
                    || readOnlyDataSourceIndexCacheCurrentValues == null
                    || readOnlyDataSourceIndexCacheCurrentValues.isEmpty()) {
                    return new WriterMethodInvokeResult(WriterMethodInvokeResult.CODE_OF_DATA_IS_EMPTY,
                                                        "target data is empty").toString();
                }
                List<WeightedDataSourceWrapper> weightedDataSourceList0 = readOnlyDataSourceIndexCacheOriginalValues.get(scName);
                List<WeightedDataSourceWrapper> weightedDataSourceList1 = readOnlyDataSourceIndexCacheCurrentValues.get(scName);
                if (weightedDataSourceList0 == null
                    || weightedDataSourceList0.isEmpty()
                    || weightedDataSourceList0.size() <= index//
                    || weightedDataSourceList1 == null || weightedDataSourceList1.isEmpty()
                    || weightedDataSourceList1.size() <= index) {
                    return new WriterMethodInvokeResult(WriterMethodInvokeResult.CODE_OF_DATA_IS_EMPTY,
                                                        "target data is empty").toString();
                }
                WeightedDataSource weightedDataSource0 = weightedDataSourceList0.get(index);
                WeightedDataSource weightedDataSource1 = weightedDataSourceList1.get(index);
                if (weightedDataSource0 == null || weightedDataSource1 == null) {
                    return new WriterMethodInvokeResult(WriterMethodInvokeResult.CODE_OF_DATA_IS_EMPTY,
                                                        "target data is empty").toString();
                }
                // ok
                weightedDataSource1.setWeight(weightedDataSource0.getWeight());
                refreshReadDataSourceQueryCache(scName, weightedDataSourceList1);
                return new WriterMethodInvokeResult(WriterMethodInvokeResult.CODE_OF_SUCCESS, "OK").toString();
            }

            @Override
            public Integer getWeight(String scName, String dataSourceName) {
                scName = DDRStringUtils.toLowerCase(scName);
                dataSourceName = DDRStringUtils.trimToNull(dataSourceName);
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
                dataSourceName = DDRStringUtils.trimToNull(dataSourceName);
                if (scName == null || dataSourceName == null || weight < 0) {
                    return new WriterMethodInvokeResult(WriterMethodInvokeResult.CODE_OF_ILLEGAL_ARGUMENT,
                                                        "illegal argument(s)").toString();
                }
                if (readOnlyDataSourceMapCahceCurrentValues == null
                    || readOnlyDataSourceMapCahceCurrentValues.isEmpty()) {
                    return new WriterMethodInvokeResult(WriterMethodInvokeResult.CODE_OF_DATA_IS_EMPTY,
                                                        "target data is empty").toString();
                }
                Map<String, WeightedDataSourceWrapper> weightedDataSourceMap = readOnlyDataSourceMapCahceCurrentValues.get(scName);
                if (weightedDataSourceMap == null || weightedDataSourceMap.isEmpty()) {
                    return new WriterMethodInvokeResult(WriterMethodInvokeResult.CODE_OF_DATA_IS_EMPTY,
                                                        "target data is empty").toString();
                }
                WeightedDataSource weightedDataSource = weightedDataSourceMap.get(dataSourceName);
                if (weightedDataSource == null) {
                    return new WriterMethodInvokeResult(WriterMethodInvokeResult.CODE_OF_DATA_IS_EMPTY,
                                                        "target data is empty").toString();
                }
                // ok
                weightedDataSource.setWeight(weight);
                refreshReadDataSourceQueryCache(scName, readOnlyDataSourceIndexCacheCurrentValues.get(scName));
                return new WriterMethodInvokeResult(WriterMethodInvokeResult.CODE_OF_SUCCESS, "OK").toString();
            }

            @Override
            public String restoreWeight(String scName, String dataSourceName) {
                scName = DDRStringUtils.toLowerCase(scName);
                dataSourceName = DDRStringUtils.trimToNull(dataSourceName);
                if (scName == null || dataSourceName == null) {
                    return new WriterMethodInvokeResult(WriterMethodInvokeResult.CODE_OF_ILLEGAL_ARGUMENT,
                                                        "illegal argument(s)").toString();
                }
                if (readOnlyDataSourceMapCacheOriginalValues == null
                    || readOnlyDataSourceMapCacheOriginalValues.isEmpty()
                    || readOnlyDataSourceMapCahceCurrentValues == null
                    || readOnlyDataSourceMapCahceCurrentValues.isEmpty()) {
                    return new WriterMethodInvokeResult(WriterMethodInvokeResult.CODE_OF_DATA_IS_EMPTY,
                                                        "target data is empty").toString();
                }
                Map<String, WeightedDataSourceWrapper> weightedDataSourceMap0 = readOnlyDataSourceMapCacheOriginalValues.get(scName);
                Map<String, WeightedDataSourceWrapper> weightedDataSourceMap1 = readOnlyDataSourceMapCahceCurrentValues.get(scName);
                if (weightedDataSourceMap0 == null || weightedDataSourceMap0.isEmpty()//
                    || weightedDataSourceMap1 == null || weightedDataSourceMap1.isEmpty()) {
                    return new WriterMethodInvokeResult(WriterMethodInvokeResult.CODE_OF_DATA_IS_EMPTY,
                                                        "target data is empty").toString();
                }
                WeightedDataSource weightedDataSource0 = weightedDataSourceMap0.get(dataSourceName);
                WeightedDataSource weightedDataSource1 = weightedDataSourceMap1.get(dataSourceName);
                if (weightedDataSource0 == null || weightedDataSource1 == null) {
                    return new WriterMethodInvokeResult(WriterMethodInvokeResult.CODE_OF_DATA_IS_EMPTY,
                                                        "target data is empty").toString();
                }
                // ok
                weightedDataSource1.setWeight(weightedDataSource0.getWeight());
                refreshReadDataSourceQueryCache(scName, readOnlyDataSourceIndexCacheCurrentValues.get(scName));
                return new WriterMethodInvokeResult(WriterMethodInvokeResult.CODE_OF_SUCCESS, "OK").toString();
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
            public String restoreWeight(String scName) {
                scName = DDRStringUtils.toLowerCase(scName);
                if (scName == null) {
                    return new WriterMethodInvokeResult(WriterMethodInvokeResult.CODE_OF_ILLEGAL_ARGUMENT,
                                                        "illegal argument(s)").toString();
                }
                if (readOnlyDataSourceIndexCacheOriginalValues == null
                    || readOnlyDataSourceIndexCacheOriginalValues.isEmpty()
                    || readOnlyDataSourceIndexCacheCurrentValues == null
                    || readOnlyDataSourceIndexCacheCurrentValues.isEmpty()) {
                    return new WriterMethodInvokeResult(WriterMethodInvokeResult.CODE_OF_DATA_IS_EMPTY,
                                                        "target data is empty").toString();
                }
                List<WeightedDataSourceWrapper> weightedDataSourceList0 = readOnlyDataSourceIndexCacheOriginalValues.get(scName);
                List<WeightedDataSourceWrapper> weightedDataSourceList1 = readOnlyDataSourceIndexCacheCurrentValues.get(scName);

                if (weightedDataSourceList0 == null || weightedDataSourceList0.isEmpty()
                    || weightedDataSourceList0 == null || weightedDataSourceList0.isEmpty()) {
                    return new WriterMethodInvokeResult(WriterMethodInvokeResult.CODE_OF_DATA_IS_EMPTY,
                                                        "target data is empty").toString();
                }
                for (int i = 0; i < weightedDataSourceList0.size(); i++) {
                    WeightedDataSource weightedDataSource0 = weightedDataSourceList0.get(i);
                    WeightedDataSource weightedDataSource1 = weightedDataSourceList1.get(i);
                    weightedDataSource1.setWeight(weightedDataSource0.getWeight());
                }
                // ok
                refreshReadDataSourceQueryCache(scName, weightedDataSourceList1);
                return new WriterMethodInvokeResult(WriterMethodInvokeResult.CODE_OF_SUCCESS, "OK").toString();
            }

            @Override
            public String restoreWeight() {
                if (readOnlyDataSourceIndexCacheOriginalValues == null
                    || readOnlyDataSourceIndexCacheOriginalValues.isEmpty()
                    || readOnlyDataSourceIndexCacheCurrentValues == null
                    || readOnlyDataSourceIndexCacheCurrentValues.isEmpty()) {
                    return new WriterMethodInvokeResult(WriterMethodInvokeResult.CODE_OF_DATA_IS_EMPTY,
                                                        "data exception").toString();
                }
                for (Map.Entry<String, List<WeightedDataSourceWrapper>> entry : readOnlyDataSourceIndexCacheOriginalValues.entrySet()) {
                    List<WeightedDataSourceWrapper> weightedDataSourceList0 = entry.getValue();
                    List<WeightedDataSourceWrapper> weightedDataSourceList1 = readOnlyDataSourceIndexCacheCurrentValues.get(entry.getKey());

                    if (weightedDataSourceList0 == null || weightedDataSourceList0.isEmpty()
                        || weightedDataSourceList0 == null || weightedDataSourceList0.isEmpty()) {
                        return new WriterMethodInvokeResult(WriterMethodInvokeResult.CODE_OF_DATA_IS_EMPTY,
                                                            "data exception").toString();
                    }
                    for (int i = 0; i < weightedDataSourceList0.size(); i++) {
                        WeightedDataSource weightedDataSource0 = weightedDataSourceList0.get(i);
                        WeightedDataSource weightedDataSource1 = weightedDataSourceList1.get(i);
                        weightedDataSource1.setWeight(weightedDataSource0.getWeight());
                    }
                }
                DefaultReadWriteDataSourceManager.this.refreshReadDataSourceQueryCache();
                return new WriterMethodInvokeResult(WriterMethodInvokeResult.CODE_OF_SUCCESS, "OK").toString();
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
                DefaultReadWriteDataSourceManager.this.refreshReadDataSourceQueryCache(readOnlyDataSourceQueryCache,
                                                                                       schema,
                                                                                       weightedDataSourceWrappers);

            }
        };
    }

    private void check(Map<String, DataSourceWrapper> writeOnlyDataSourceQueryCache) {
        if (writeOnlyDataSourceQueryCache == null || writeOnlyDataSourceQueryCache.isEmpty() || metaDataChecker == null
            || shardRouter == null) {
            return;
        }
        Map<String, Set<String>> routedTables = shardRouter.getRoutedTables();
        if (routedTables == null || routedTables.isEmpty()) {
            return;
        }
        for (Map.Entry<String, DataSourceWrapper> entry : writeOnlyDataSourceQueryCache.entrySet()) {
            Connection conn = null;
            try {
                conn = entry.getValue().getDataSource().getConnection();
                String scName = entry.getKey();
                check(conn, scName);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            } finally {
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        // igonre;
                    }
                }
            }
        }
    }

    private void check(LinkedHashMap<String, List<WeightedDataSourceWrapper>> readOnlyDataSourceIndexCacheOriginalValues) {
        if (readOnlyDataSourceIndexCacheOriginalValues == null || readOnlyDataSourceIndexCacheOriginalValues.isEmpty()) {
            return;
        }
        for (Map.Entry<String, List<WeightedDataSourceWrapper>> entry : readOnlyDataSourceIndexCacheOriginalValues.entrySet()) {
            for (WeightedDataSourceWrapper dataSource : entry.getValue()) {
                Connection conn = null;
                try {
                    conn = dataSource.getDataSource().getConnection();
                    String scName = entry.getKey();
                    check(conn, scName);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                } finally {
                    if (conn != null) {
                        try {
                            conn.close();
                        } catch (SQLException e) {
                            // igonre;
                        }
                    }
                }
            }
        }
    }

    private Map<String, Set<String>> getPhysicalTables() {
        if (physicalTables == null) {
            synchronized (this) {
                if (physicalTables == null) {
                    Map<String, Set<String>> tabs = new HashMap<>();
                    Map<String, Set<String>> routedTables = shardRouter.getRoutedTables();
                    if (routedTables != null) {
                        for (Map.Entry<String, Set<String>> entry : routedTables.entrySet()) {
                            String sc = entry.getKey();
                            for (String tb : entry.getValue()) {
                                List<ShardRouteInfo> routeInfos = shardRouter.getRouteInfos(sc, tb);
                                if (routeInfos != null) {
                                    for (ShardRouteInfo routeInfo : routeInfos) {
                                        Set<String> set = tabs.get(routeInfo.getScName());
                                        if (set == null) {
                                            set = new HashSet<>();
                                            tabs.put(routeInfo.getScName(), set);
                                        }
                                        set.add(routeInfo.getTbName());
                                    }
                                }
                            }
                        }
                    }
                    physicalTables = tabs;
                }
            }
        }
        return physicalTables;
    }

    private void check(Connection conn, String scName) {
        if (metaDataChecker == null || shardRouter == null) {
            return;
        }
        Map<String, Set<String>> physicalTables = getPhysicalTables();
        if (physicalTables != null) {
            Set<String> tbNames = physicalTables.get(scName);
            if (tbNames != null && !tbNames.isEmpty()) {
                metaDataChecker.check(conn, scName, tbNames);
                if (stdLogger.isInfoEnabled()) {
                    stdLogger.info("MetaDataCheck - sc:{}, tb:{} meta data checking is passed", scName, tbNames);
                }
            }
        }
    }

    private void initWriteOnlyDataSource(List<WriteOnlyDataSourceBinding> bindings) {
        if (bindings == null || bindings.isEmpty()) {
            return;
        }
        final Map<String, DataSourceWrapper> dataSourceMap = new HashMap<String, DataSourceWrapper>();
        for (final WriteOnlyDataSourceBinding binding : bindings) {
            String schemasString = DDRStringUtils.trimToNull(binding.getScNames());
            if (schemasString == null) {
                throw new IllegalArgumentException("scNames of 'writeOnlyDataSourceQueryCache' can't be empty");
            }
            final List<String> schemas = new ArrayList<>();
            new RangeExpressionParser(schemasString).visit(new RangeExpressionItemVisitor() {

                @Override
                public void visit(Object val) {
                    schemas.add(val.toString());
                }
            });
            buildWriteOnlyDataSource(dataSourceMap, schemas, binding.getDataSource());
        }
        this.writeOnlyDataSourceQueryCache = dataSourceMap;
    }

    private void buildWriteOnlyDataSource(Map<String, DataSourceWrapper> dataSourceMap, List<String> schemas,
                                          DataSource dataSource) {
        Set<String> uniqueSchemas = new HashSet<>();
        if (schemas != null && !schemas.isEmpty()) {
            for (String s : schemas) {
                uniqueSchemas.add(s);
            }
        }
        for (String schema : schemas) {
            schema = DDRStringUtils.trimToNull(schema);
            if (schema == null) {
                throw new IllegalArgumentException("Schema of 'writeOnlyDataSources' can't be null");
            }
            if (dataSource == null) {
                throw new IllegalArgumentException("[schema:" + schema
                                                   + "] dataSource of 'writeOnlyDataSources' can't be null");
            }
            schema = DDRStringUtils.toLowerCase(schema);
            boolean exist = dataSourceMap.containsKey(schema);
            if (exist) {
                throw new IllegalArgumentException("Schema '" + schema
                                                   + "' duplicate binding in 'writeOnlyDataSources' configuration");
            } else {
                dataSourceMap.put(schema, new DataSourceWrapper(dataSource, uniqueSchemas));
            }
        }
    }

    private void initReadOnlyDataSource(List<ReadOnlyDataSourceBinding> bindings) {
        if (bindings == null || bindings.isEmpty()) {
            return;
        }
        final LinkedHashMap<String, List<WeightedDataSourceWrapper>> readOnlyDataSourceIndexCacheOriginalValues = new LinkedHashMap<String, List<WeightedDataSourceWrapper>>();
        for (final ReadOnlyDataSourceBinding binding : bindings) {
            String schemasString = DDRStringUtils.trimToNull(binding.getScNames());
            if (schemasString == null) {
                throw new IllegalArgumentException("scNames of 'readOnlyDataSourceQueryCache' can't be empty");
            }
            final List<String> schemas = new ArrayList<>();
            new RangeExpressionParser(schemasString).visit(new RangeExpressionItemVisitor() {

                @Override
                public void visit(Object val) {
                    schemas.add(val.toString());
                }
            });
            buildReadOnlyDataSource(readOnlyDataSourceIndexCacheOriginalValues, schemas, binding.getDataSources());
        }
        //
        this.readOnlyDataSourceIndexCacheOriginalValues = readOnlyDataSourceIndexCacheOriginalValues;
        if (readOnlyDataSourceIndexCacheOriginalValues == null) {
            this.readOnlyDataSourceIndexCacheCurrentValues = null;// list
            this.readOnlyDataSourceMapCahceCurrentValues = null;// map
            this.readOnlyDataSourceMapCacheOriginalValues = null;// map
        } else {
            LinkedHashMap<String, List<WeightedDataSourceWrapper>> curList = new LinkedHashMap<String, List<WeightedDataSourceWrapper>>();
            LinkedHashMap<String, Map<String, WeightedDataSourceWrapper>> curMap = new LinkedHashMap<String, Map<String, WeightedDataSourceWrapper>>();
            LinkedHashMap<String, Map<String, WeightedDataSourceWrapper>> orgMap = new LinkedHashMap<String, Map<String, WeightedDataSourceWrapper>>();
            for (Map.Entry<String, List<WeightedDataSourceWrapper>> entry : readOnlyDataSourceIndexCacheOriginalValues.entrySet()) {
                if (entry.getValue() == null) {
                    curList.put(entry.getKey(), null);
                } else {
                    List<WeightedDataSourceWrapper> list = new ArrayList<WeightedDataSourceWrapper>(
                                                                                                    entry.getValue().size());
                    Map<String, WeightedDataSourceWrapper> map0 = new LinkedHashMap<String, WeightedDataSourceWrapper>();
                    Map<String, WeightedDataSourceWrapper> map1 = new LinkedHashMap<String, WeightedDataSourceWrapper>();
                    for (WeightedDataSourceWrapper weightedDataSourceWrapper : entry.getValue()) {
                        WeightedDataSourceWrapper backup = weightedDataSourceWrapper.clone();
                        list.add(backup);
                        if (backup.getName() != null) {
                            map0.put(weightedDataSourceWrapper.getName(), backup);
                            map1.put(weightedDataSourceWrapper.getName(), weightedDataSourceWrapper);
                        }
                    }
                    if (!list.isEmpty()) {// deep copy
                        curList.put(entry.getKey(), list);
                    }
                    if (!map0.isEmpty()) {// deep copy
                        curMap.put(entry.getKey(), map0);
                    }
                    if (!map1.isEmpty()) {// normal copy
                        orgMap.put(entry.getKey(), map1);
                    }
                }
            }
            this.readOnlyDataSourceIndexCacheCurrentValues = curList;// list
            this.readOnlyDataSourceMapCahceCurrentValues = curMap;// map
            this.readOnlyDataSourceMapCacheOriginalValues = orgMap;// map
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
                WeightItem weightItem = new WeightItem(weightedDataSourceWrapper.getWeight(), weightedDataSourceWrapper);
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
                    throw new IllegalArgumentException("scName can't be empty");
                } else if (uniqSchemas.contains(s)) {
                    throw new IllegalArgumentException("duplicate scName '" + s + "'");
                } else {
                    uniqSchemas.add(s);
                }
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
                throw new IllegalArgumentException("Duplicate schema '" + schema
                                                   + "' bind in 'readOnlyDataSources' configuration");
            } else {
                List<WeightItem> itemList = new ArrayList<WeightItem>();
                List<WeightedDataSourceWrapper> dataSourceSchemasBindings = new ArrayList<WeightedDataSourceWrapper>();
                Set<String> nameSet = new HashSet<String>();
                for (int i = 0; i < dataSources.size(); i++) {
                    WeightedDataSource weightedDataSource = dataSources.get(i);
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
                    WeightedDataSourceWrapper weightedDataSourceWrapper = new WeightedDataSourceWrapper();
                    weightedDataSourceWrapper.setDataSource(weightedDataSource.getDataSource());
                    weightedDataSourceWrapper.setName(DDRStringUtils.trimToNull(weightedDataSource.getName()));
                    weightedDataSourceWrapper.setIndex(i);
                    weightedDataSourceWrapper.setDesc(weightedDataSource.getDesc());
                    weightedDataSourceWrapper.setWeight(weightedDataSource.getWeight());
                    weightedDataSourceWrapper.setDataSourceWrapper(new DataSourceWrapper(
                                                                                         weightedDataSourceWrapper.getDataSource(),
                                                                                         uniqSchemas));
                    // check
                    if (weightedDataSourceWrapper.getName() != null) {
                        if (nameSet.contains(weightedDataSourceWrapper.getName())) {
                            throw new IllegalArgumentException("Duplicate datasource name '"
                                                               + weightedDataSourceWrapper.getName()
                                                               + "' for scNames '"
                                                               + DDRJSONUtils.toJSONString(uniqSchemas) + "'");
                        } else {
                            nameSet.add(weightedDataSourceWrapper.getName());
                        }
                    }
                    // add
                    dataSourceSchemasBindings.add(weightedDataSourceWrapper);
                    if (weightedDataSource.getWeight() == 0) {
                        continue;
                    } else {
                        WeightItem weightItem = new WeightItem();
                        weightItem.setWeight(weightedDataSource.getWeight());
                        weightItem.setValue(weightedDataSourceWrapper);
                        itemList.add(weightItem);
                    }
                }
                schemaDataSourceMapping.put(schema, dataSourceSchemasBindings);
            }
        }
    }

    @Override
    public DataSourceWrapper getDataSource(DataSourceParam param) {
        init();
        if (param.getScNames() == null || param.getScNames().isEmpty()) {
            throw new IllegalArgumentException("scNames can't be empty");
        }
        boolean readOnly = param.isReadOnly();
        if (readOnly) {
            if (this.readOnlyDataSourceQueryCache == null) {
                throw new DataSourceNotFoundException("No 'readOnlyDataSource' is configured");
            } else {
                WeightedDataSourceWrapper weightedDataSourceWrapper = null;
                for (String scName : param.getScNames()) {
                    if (weightedDataSourceWrapper == null) {
                        WeightedRandom weightedRandom = this.readOnlyDataSourceQueryCache.get(scName);
                        if (weightedRandom == null) {
                            throw new DataSourceNotFoundException("schema:'" + scName
                                                                  + "' isn't configured in 'readOnlyDataSource' list ");
                        } else {
                            weightedDataSourceWrapper = (WeightedDataSourceWrapper) weightedRandom.nextValue();
                        }
                    } else {
                        if (!weightedDataSourceWrapper.getDataSourceWrapper().getSchemas().contains(scName)) {
                            throw new CrossDataSourceException(
                                                               "For parameter "
                                                                       + param
                                                                       + ", scName:'"
                                                                       + scName
                                                                       + "' is not in 'readOnlyDataSource' binding '"
                                                                       + weightedDataSourceWrapper.getDataSourceWrapper().toString()
                                                                       + "'");
                        }
                    }
                }
                // log
                if (stdLogger.isDebugEnabled()) {
                    stdLogger.debug(new StringBuilder("[GetDataSource] ")//
                    .append("param:")//
                    .append(param)//
                    .append(" matched R:")//
                    .append(weightedDataSourceWrapper)//
                    .toString());
                }
                return weightedDataSourceWrapper.getDataSourceWrapper();
            }
        } else {
            if (this.writeOnlyDataSourceQueryCache == null) {
                throw new DataSourceNotFoundException("No 'writeOnlyDataSource' is configured");
            } else {
                DataSourceWrapper dataSourceWrapper = null;
                for (String scName : param.getScNames()) {
                    if (dataSourceWrapper == null) {
                        dataSourceWrapper = this.writeOnlyDataSourceQueryCache.get(scName);
                        if (dataSourceWrapper == null) {
                            throw new DataSourceNotFoundException("schema '" + scName
                                                                  + "' isn't configured in 'writeOnlyDataSource' list");
                        }
                    } else {
                        if (!dataSourceWrapper.getSchemas().contains(scName)) {
                            throw new CrossDataSourceException("For parameter " + param + ", scName:'" + scName
                                                               + "' is not in 'writeOnlyDataSource' binding '"
                                                               + dataSourceWrapper.toString() + "'");
                        }
                    }
                }
                // log
                if (stdLogger.isDebugEnabled()) {
                    stdLogger.debug(new StringBuilder("[GetDataSource] ")//
                    .append("param:")//
                    .append(param)//
                    .append(" matched W:")//
                    .append(dataSourceWrapper)//
                    .toString());
                }
                return dataSourceWrapper;
            }
        }
    }

}
