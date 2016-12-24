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
package org.hellojavaer.ddr.core.sharding;

import org.hellojavaer.ddr.core.datasource.exception.AmbiguousDataSourceException;
import org.hellojavaer.ddr.core.utils.DDRStringUtils;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 15/11/2016.
 */
public class ShardingRouteContext {

    private static final ThreadLocal<Map<String, InnerShadingInfoWrapper>> ROUTE_VALUE         = new ThreadLocal<Map<String, InnerShadingInfoWrapper>>() {

                                                                                                   protected Map<String, InnerShadingInfoWrapper> initialValue() {
                                                                                                       return new HashMap<String, InnerShadingInfoWrapper>();
                                                                                                   }
                                                                                               };
    private static final ThreadLocal<Boolean>                              DISABLE_SQL_ROUTING = new ThreadLocal<Boolean>();

    public static void setDisableSqlRouting(boolean disableSqlRoute) {
        if (disableSqlRoute) {
            DISABLE_SQL_ROUTING.set(Boolean.TRUE);
        }
    }

    public static boolean isDisableSqlRouting() {
        return DISABLE_SQL_ROUTING.get() == Boolean.TRUE;
    }

    /**
     * 设置路由信息
     * 相同路由,采用覆盖原则
     * 该路由信息在以下情况会被使用
     * 1.sql中没有设置sdValue (如果设置了但为null不会使用),
     * 2.设置DisableSqlRouting为true
     */
    public static void setRoute(String scName, String tbName, Object value) {
        scName = DDRStringUtils.toLowerCase(scName);
        tbName = DDRStringUtils.toLowerCase(tbName);
        if (scName == null) {
            throw new IllegalArgumentException("'scName' can't be null");
        }
        if (tbName == null) {
            throw new IllegalArgumentException("'tbName' can't be null");
        }
        Map<String, InnerShadingInfoWrapper> map = ROUTE_VALUE.get();
        String fullKey = buildQueryKey(scName, tbName);
        InnerShadingInfoWrapper innerShadingInfoWrapper = map.get(fullKey);
        if (innerShadingInfoWrapper != null) {
            InnerShadingInfoWrapper tableNameShadingInfo = map.get(tbName);
            if (tableNameShadingInfo.getConflictSchemas().size() > 1) {
                tableNameShadingInfo.getConflictSchemas().remove(scName);
            } else {
                map.remove(tbName);
            }
        }
        // 覆盖
        map.put(fullKey, new InnerShadingInfoWrapper(scName, value));
        InnerShadingInfoWrapper tableNameShadingInfo = map.get(tbName);
        if (tableNameShadingInfo != null) {
            tableNameShadingInfo.getConflictSchemas().add(scName);
        } else {
            map.put(tbName, new InnerShadingInfoWrapper(scName, value));
        }
    }

    /**
     * 设置路由信息
     * 相同路由,采用覆盖原则
     * 该路由信息在以下情况会被使用
     * 1.sql中没有设置sdValue (如果设置了但为null不会使用),
     * 2.设置DisableSqlRouting为true
     */
    public static void setRoute(String scName, String tbName, RouteInfo value) {
        setRoute(scName, tbName, (Object) value);
    }

    /**
     * 
     * 删除路由信息
     */
    public static void removeRoute(String scName, String tbName) {
        scName = DDRStringUtils.toLowerCase(scName);
        tbName = DDRStringUtils.toLowerCase(tbName);
        if (scName == null) {
            throw new IllegalArgumentException("'scName' can't be null");
        }
        if (tbName == null) {
            throw new IllegalArgumentException("'tbName' can't be null");
        }

        Map<String, InnerShadingInfoWrapper> map = ROUTE_VALUE.get();
        map.remove(buildQueryKey(scName, tbName));
        InnerShadingInfoWrapper tableNameShadingInfo = map.get(tbName);
        if (tableNameShadingInfo != null) {
            if (tableNameShadingInfo.conflictSchemas.size() > 1) {
                tableNameShadingInfo.conflictSchemas.remove(scName);
            } else {
                map.remove(tbName);
            }
        }
    }

    public static Object getRoute(String scName, String tbName) {
        scName = DDRStringUtils.toLowerCase(scName);
        tbName = DDRStringUtils.toLowerCase(tbName);
        if (tbName == null) {
            throw new IllegalArgumentException("'tbName' can't be null");
        }
        Map<String, InnerShadingInfoWrapper> map = ROUTE_VALUE.get();
        InnerShadingInfoWrapper shadingInfoWrapper = map.get(buildQueryKey(scName, tbName));
        if (shadingInfoWrapper == null) {
            return null;
        } else if (shadingInfoWrapper.getConflictSchemas().size() > 1) {
            throw new AmbiguousDataSourceException("Datasource binding for scName:" + scName + ", tbName:" + tbName
                                                   + " is ambiguous");
        } else {
            return shadingInfoWrapper.getShardingInfo();
        }
    }

    private static String buildQueryKey(String scName, String tbName) {
        if (scName == null) {
            return tbName;
        } else {
            return new StringBuilder().append(scName).append('.').append(tbName).toString();
        }
    }

    public static void clear() {
        ROUTE_VALUE.remove();
        DISABLE_SQL_ROUTING.remove();
    }

    protected static class InnerShadingInfoWrapper {

        private List<String> conflictSchemas = new LinkedList<String>();
        private Object       shardingInfo;

        public InnerShadingInfoWrapper(String scName, Object shardingInfo) {
            this.conflictSchemas.add(scName);
            this.shardingInfo = shardingInfo;
        }

        public List<String> getConflictSchemas() {
            return conflictSchemas;
        }

        public void setConflictSchemas(List<String> conflictSchemas) {
            this.conflictSchemas = conflictSchemas;
        }

        public Object getShardingInfo() {
            return shardingInfo;
        }

        public void setShardingInfo(Object shardingInfo) {
            this.shardingInfo = shardingInfo;
        }
    }

}
