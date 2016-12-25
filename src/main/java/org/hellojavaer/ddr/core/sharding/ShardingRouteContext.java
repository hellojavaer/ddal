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

    private static final ThreadLocal<Map<String, InnerRouteInfoWrapper>> ROUTE_VALUE         = new ThreadLocal<Map<String, InnerRouteInfoWrapper>>() {

                                                                                                   protected Map<String, InnerRouteInfoWrapper> initialValue() {
                                                                                                       return new HashMap<String, InnerRouteInfoWrapper>();
                                                                                                   }
                                                                                               };
    private static final ThreadLocal<Boolean>                            DISABLE_SQL_ROUTING = new ThreadLocal<Boolean>();

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
    public static void setRouteInfo(String scName, String tbName, Object value) {
        scName = DDRStringUtils.toLowerCase(scName);
        tbName = DDRStringUtils.toLowerCase(tbName);
        if (scName == null) {
            throw new IllegalArgumentException("'scName' can't be null");
        }
        if (tbName == null) {
            throw new IllegalArgumentException("'tbName' can't be null");
        }
        Map<String, InnerRouteInfoWrapper> map = ROUTE_VALUE.get();
        String fullKey = buildQueryKey(scName, tbName);
        InnerRouteInfoWrapper innerRouteInfoWrapper = map.get(fullKey);
        if (innerRouteInfoWrapper != null) {
            InnerRouteInfoWrapper tableNameShadingInfo = map.get(tbName);
            if (tableNameShadingInfo.getConflictSchemas().size() > 1) {
                tableNameShadingInfo.getConflictSchemas().remove(scName);
            } else {
                map.remove(tbName);
            }
        }
        // 覆盖
        map.put(fullKey, new InnerRouteInfoWrapper(scName, value));
        InnerRouteInfoWrapper tableNameShadingInfo = map.get(tbName);
        if (tableNameShadingInfo != null) {
            tableNameShadingInfo.getConflictSchemas().add(scName);
        } else {
            map.put(tbName, new InnerRouteInfoWrapper(scName, value));
        }
    }

    /**
     * 设置路由信息
     * 相同路由,采用覆盖原则
     * 该路由信息在以下情况会被使用
     * 1.sql中没有设置sdValue (如果设置了但为null不会使用),
     * 2.设置DisableSqlRouting为true
     */
    public static void setRouteInfo(String scName, String tbName, RouteInfo value) {
        setRouteInfo(scName, tbName, (Object) value);
    }

    /**
     * 
     * 删除路由信息
     */
    public static void removeRouteInfo(String scName, String tbName) {
        scName = DDRStringUtils.toLowerCase(scName);
        tbName = DDRStringUtils.toLowerCase(tbName);
        if (scName == null) {
            throw new IllegalArgumentException("'scName' can't be null");
        }
        if (tbName == null) {
            throw new IllegalArgumentException("'tbName' can't be null");
        }

        Map<String, InnerRouteInfoWrapper> map = ROUTE_VALUE.get();
        map.remove(buildQueryKey(scName, tbName));
        InnerRouteInfoWrapper tableNameShadingInfo = map.get(tbName);
        if (tableNameShadingInfo != null) {
            if (tableNameShadingInfo.conflictSchemas.size() > 1) {
                tableNameShadingInfo.conflictSchemas.remove(scName);
            } else {
                map.remove(tbName);
            }
        }
    }

    public static Object getRouteInfo(String scName, String tbName) {
        scName = DDRStringUtils.toLowerCase(scName);
        tbName = DDRStringUtils.toLowerCase(tbName);
        if (tbName == null) {
            throw new IllegalArgumentException("'tbName' can't be null");
        }
        Map<String, InnerRouteInfoWrapper> map = ROUTE_VALUE.get();
        InnerRouteInfoWrapper routeInfoWrapper = map.get(buildQueryKey(scName, tbName));
        if (routeInfoWrapper == null) {
            return null;
        } else if (routeInfoWrapper.getConflictSchemas().size() > 1) {
            throw new AmbiguousDataSourceException("Datasource binding for scName:" + scName + ", tbName:" + tbName
                                                   + " is ambiguous");
        } else {
            return routeInfoWrapper.getRouteInfo();
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

    protected static class InnerRouteInfoWrapper {

        private List<String> conflictSchemas = new LinkedList<String>();
        private Object       routeInfo;

        public InnerRouteInfoWrapper(String scName, Object routeInfo) {
            this.conflictSchemas.add(scName);
            this.routeInfo = routeInfo;
        }

        public List<String> getConflictSchemas() {
            return conflictSchemas;
        }

        public void setConflictSchemas(List<String> conflictSchemas) {
            this.conflictSchemas = conflictSchemas;
        }

        public Object getRouteInfo() {
            return routeInfo;
        }

        public void setRouteInfo(Object routeInfo) {
            this.routeInfo = routeInfo;
        }
    }

}
