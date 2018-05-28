/*
 * Copyright 2017-2017 the original author or authors.
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
package org.hellojavaer.ddal.ddr.shard;

import org.hellojavaer.ddal.ddr.datasource.jdbc.DDRDataSource;
import org.hellojavaer.ddal.ddr.datasource.jdbc.DataSourceWrapper;
import org.hellojavaer.ddal.ddr.datasource.manager.DataSourceParam;

import java.util.*;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 20/06/2017.
 */
public class ShardRouteUtils {

    public static Map<ShardRouteInfo, List<RangeShardValue>> groupSdValuesByRouteInfo(ShardRouter shardRouter,
                                                                                      String scName, String tbName,
                                                                                      RangeShardValue sdValues) {
        ShardRouteConfig routeConfig = shardRouter.getRouteConfig(scName, tbName);
        ShardRouteRule rule = shardRouter.getRouteRule(routeConfig.getScName(), routeConfig.getTbName());
        Map<ShardRouteInfo, List<RangeShardValue>> map = rule.groupSdValuesByRouteInfo(scName, tbName, sdValues);
        if (map == null) {
            return Collections.emptyMap();
        }
        return map;
    }

    public static <T> Map<ShardRouteInfo, List<T>> groupSdValuesByRouteInfo(ShardRouter shardRouter, String scName,
                                                                            String tbName, List<T> sdValues) {
        if (sdValues == null || sdValues.isEmpty()) {
            return Collections.EMPTY_MAP;
        }
        Map<ShardRouteInfo, List<T>> map = new LinkedHashMap<>();
        for (Object item : sdValues) {
            ShardRouteInfo routeInfo = shardRouter.getRouteInfo(scName, tbName, item);
            List list = map.get(routeInfo);
            if (list == null) {
                list = new ArrayList();
                map.put(routeInfo, list);
            }
            list.add(item);
        }
        return map;
    }

    public static <T> Map<ShardRouteInfo, Set<T>> groupSdValuesByRouteInfo(ShardRouter shardRouter, String scName,
                                                                           String tbName, Set<T> sdValues) {
        if (sdValues == null || sdValues.isEmpty()) {
            return Collections.EMPTY_MAP;
        }
        Map<ShardRouteInfo, Set<T>> map = null;
        if (sdValues instanceof LinkedHashSet) {
            map = new LinkedHashMap();
        } else {
            map = new HashMap<>();
        }
        for (Object item : sdValues) {
            ShardRouteInfo routeInfo = shardRouter.getRouteInfo(scName, tbName, item);
            Set set = map.get(routeInfo);
            if (set == null) {
                set = new LinkedHashSet();
                map.put(routeInfo, set);
            }
            set.add(item);
        }
        return map;
    }

    public static Map<String, List<ShardRouteInfo>> groupRouteInfosByScName(List<ShardRouteInfo> routeInfos) {
        if (routeInfos == null || routeInfos.isEmpty()) {
            return Collections.EMPTY_MAP;
        }
        Map<String, List<ShardRouteInfo>> map = new LinkedHashMap<>();
        for (ShardRouteInfo routeInfo : routeInfos) {
            List<ShardRouteInfo> list = map.get(routeInfo.getScName());
            if (list == null) {
                list = new ArrayList<>();
                map.put(routeInfo.getScName(), list);
            }
            list.add(routeInfo);
        }
        return map;
    }

    public static Map<String, Set<ShardRouteInfo>> groupRouteInfosByScName(Set<ShardRouteInfo> routeInfos) {
        if (routeInfos == null || routeInfos.isEmpty()) {
            return Collections.EMPTY_MAP;
        }
        Map<String, Set<ShardRouteInfo>> map = null;
        if (routeInfos instanceof LinkedHashSet) {
            map = new LinkedHashMap();
        } else {
            map = new HashMap<>();
        }
        for (ShardRouteInfo routeInfo : routeInfos) {
            Set<ShardRouteInfo> set = map.get(routeInfo.getScName());
            if (set == null) {
                set = new LinkedHashSet<>();
                map.put(routeInfo.getScName(), set);
            }
            set.add(routeInfo);
        }
        return map;
    }

    /**
     * 该方法通过把分片信息按数据源进行分组用以解决分布式事务的问题
     * 
     */
    public static Map<DataSourceWrapper, List<ShardRouteInfo>> groupRouteInfosByDataSource(DDRDataSource ddrDataSource,
                                                                                           boolean readOnly,
                                                                                           List<ShardRouteInfo> routeInfos) {
        if (routeInfos == null || routeInfos.isEmpty()) {
            return Collections.EMPTY_MAP;
        }
        Map<DataSourceWrapper, List<ShardRouteInfo>> result = new LinkedHashMap<>();
        Map<String, DataSourceWrapper> dataSourceWrapperMap = new HashMap<>();
        for (ShardRouteInfo routeInfo : routeInfos) {
            DataSourceWrapper dataSourceWrapper = dataSourceWrapperMap.get(routeInfo.getScName());
            if (dataSourceWrapper == null) {
                DataSourceParam param = new DataSourceParam();
                Set<String> schemas = new HashSet<>();
                schemas.add(routeInfo.getScName());
                param.setScNames(schemas);
                param.setReadOnly(readOnly);
                dataSourceWrapper = ddrDataSource.getDataSource(param);
                for (String scName : dataSourceWrapper.getSchemas()) {
                    Object preValue = dataSourceWrapperMap.put(scName, dataSourceWrapper);
                    if (preValue != null) {
                        throw new IllegalStateException("Duplicate dataSource binding on schema: " + scName);
                    }
                }
            }
            List<ShardRouteInfo> list = result.get(dataSourceWrapper);
            if (list == null) {
                list = new ArrayList<>();
                result.put(dataSourceWrapper, list);
            }
            list.add(routeInfo);
        }
        return result;
    }

    /**
     * 该方法通过把分片信息按数据源进行分组用以解决分布式事务的问题
     *
     */
    public static Map<DataSourceWrapper, Set<ShardRouteInfo>> groupRouteInfosByDataSource(DDRDataSource ddrDataSource,
                                                                                          boolean readOnly,
                                                                                          Set<ShardRouteInfo> routeInfos) {
        if (routeInfos == null || routeInfos.isEmpty()) {
            return Collections.EMPTY_MAP;
        }
        Map<DataSourceWrapper, Set<ShardRouteInfo>> result = null;
        if (routeInfos instanceof HashSet) {
            result = new LinkedHashMap<>();
        } else {
            result = new HashMap<>();
        }
        Map<String, DataSourceWrapper> dataSourceWrapperMap = new HashMap<>();
        for (ShardRouteInfo routeInfo : routeInfos) {
            DataSourceWrapper dataSourceWrapper = dataSourceWrapperMap.get(routeInfo.getScName());
            if (dataSourceWrapper == null) {
                DataSourceParam param = new DataSourceParam();
                Set<String> schemas = new HashSet<>();
                schemas.add(routeInfo.getScName());
                param.setScNames(schemas);
                param.setReadOnly(readOnly);
                dataSourceWrapper = ddrDataSource.getDataSource(param);
                for (String scName : dataSourceWrapper.getSchemas()) {
                    Object preValue = dataSourceWrapperMap.put(scName, dataSourceWrapper);
                    if (preValue != null) {
                        throw new IllegalStateException("Duplicate dataSource binding on schema: " + scName);
                    }
                }
            }
            Set<ShardRouteInfo> list = result.get(dataSourceWrapper);
            if (list == null) {
                if (routeInfos instanceof HashSet) {
                    list = new LinkedHashSet<>();
                } else {
                    list = new HashSet<>();
                }
                result.put(dataSourceWrapper, list);
            }
            list.add(routeInfo);
        }
        return result;
    }
}
