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
package org.hellojavaer.ddal.ddr.shard;

import org.hellojavaer.ddal.ddr.datasource.exception.AmbiguousDataSourceBindingException;
import org.hellojavaer.ddal.ddr.utils.DDRStringUtils;

import java.util.*;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 15/11/2016.
 */
public class ShardRouteContext {

    private static final ThreadLocal<LinkedList<SubContext>> STACK = new ThreadLocal<LinkedList<SubContext>>() {

                                                                       protected LinkedList<SubContext> initialValue() {
                                                                           LinkedList stack = new LinkedList<SubContext>();
                                                                           stack.add(new SubContext());
                                                                           return stack;
                                                                       }
                                                                   };

    public static void pushSubContext() {
        STACK.get().addFirst(new SubContext());
    }

    public static void popSubContext() {
        if (STACK.get().size() <= 1) {
            throw new IndexOutOfBoundsException("No sub context in the stack");
        } else {
            STACK.get().removeFirst();
        }
    }

    /**
     * 
     * @param key
     * @param val
     */
    public static void setParameter(String key, Object val) {
        if (val == null) {
            val = NULL_OBJECT;
        }
        getCurContext().getVarContext().put(key, val);
    }

    public static Object getParameter(String key) {
        for (SubContext context : STACK.get()) {
            Map<String, Object> varContext = context.getVarContext();
            Object val = varContext.get(key);
            if (val == null) {
                continue;
            } else if (val == NULL_OBJECT) {
                return null;
            } else {
                return val;
            }
        }
        return null;
    }

    public static boolean containsParameter(String key) {
        for (SubContext context : STACK.get()) {
            Map<String, Object> varContext = context.getVarContext();
            Object val = varContext.get(key);
            if (val == null) {
                continue;
            } else {
                return true;
            }
        }
        return false;
    }

    public static Object removeParameter(String key) {
        return getCurContext().getVarContext().remove(key);
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
        if (value == null) {
            value = NULL_OBJECT;
        }
        Map<String, Map<String, Object>> map = getCurContext().getRouteContext();
        /**
         * 添加两条查询索引 
         * schema_name.table_name <-> routeInfo
         * table_name <-> routeInfo
         */
        Map<String, Object> schemaTableMap = new HashMap<String, Object>();
        schemaTableMap.put(scName, value);
        map.put(buildQueryKey(scName, tbName), schemaTableMap);

        Map<String, Object> tableMap = map.get(tbName);
        if (tableMap == null) {
            tableMap = new HashMap<String, Object>();
            map.put(tbName, tableMap);
        }
        tableMap.put(scName, value);
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
        Map<String, Map<String, Object>> map = getCurContext().getRouteContext();
        // level 1
        map.remove(buildQueryKey(scName, tbName));
        // level 2
        Map<String, Object> routeInfoMap = map.get(tbName);
        if (routeInfoMap != null && !routeInfoMap.isEmpty()) {
            routeInfoMap.remove(scName);
        }
    }

    public static Object getRouteInfo(String scName, String tbName) {
        scName = DDRStringUtils.toLowerCase(scName);
        tbName = DDRStringUtils.toLowerCase(tbName);
        if (tbName == null) {
            throw new IllegalArgumentException("'tbName' can't be null");
        }
        for (SubContext context : STACK.get()) {
            Map<String, Map<String, Object>> map = context.getRouteContext();
            String key = buildQueryKey(scName, tbName);
            Map<String, Object> routeInfoMap = map.get(key);
            if (routeInfoMap == null || routeInfoMap.isEmpty()) {
                continue;
            } else if (routeInfoMap.size() > 1) {
                throw new AmbiguousDataSourceBindingException("Datasource binding for scName:" + scName + ", tbName:"
                                                              + tbName + " is ambiguous");
            } else {
                Object object = routeInfoMap.values().iterator().next();
                if (object == NULL_OBJECT) {
                    return null;
                } else {
                    return object;
                }
            }
        }
        return null;
    }

    public static boolean containsRouteInfo(String scName, String tbName) {
        scName = DDRStringUtils.toLowerCase(scName);
        tbName = DDRStringUtils.toLowerCase(tbName);
        if (tbName == null) {
            throw new IllegalArgumentException("'tbName' can't be null");
        }
        for (SubContext context : STACK.get()) {
            Map<String, Map<String, Object>> map = context.getRouteContext();
            String key = buildQueryKey(scName, tbName);
            Map<String, Object> routeInfoMap = map.get(key);
            if (routeInfoMap == null || routeInfoMap.isEmpty()) {
                continue;
            } else {
                return true;
            }
        }
        return false;
    }

    private static String buildQueryKey(String scName, String tbName) {
        if (scName == null) {
            return tbName;
        } else {
            return new StringBuilder().append(scName).append('.').append(tbName).toString();
        }
    }

    private static SubContext getCurContext() {
        LinkedList<SubContext> linkedList = STACK.get();
        return linkedList.getFirst();
    }

    public static void clear() {
        LinkedList<SubContext> linkedList = STACK.get();
        linkedList.clear();
        linkedList.add(new SubContext());// push to context
    }

    private static final Object NULL_OBJECT = new Object();

    protected static class InnerRouteInfoWrapper {

        private Set<String> conflictSchemas = new HashSet<>();
        private Object      routeInfo;

        public InnerRouteInfoWrapper(String scName, Object routeInfo) {
            this.conflictSchemas.add(scName);
            this.routeInfo = routeInfo;
        }

        public Set<String> getConflictSchemas() {
            return conflictSchemas;
        }

        public void setConflictSchemas(Set<String> conflictSchemas) {
            this.conflictSchemas = conflictSchemas;
        }

        public Object getRouteInfo() {
            return routeInfo;
        }

        public void setRouteInfo(Object routeInfo) {
            this.routeInfo = routeInfo;
        }
    }

    private static class SubContext {

        private Map<String, Object>              varContext   = new HashMap<>();
        private Map<String, Map<String, Object>> routeContext = new HashMap<String, Map<String, Object>>();

        public Map<String, Map<String, Object>> getRouteContext() {
            return routeContext;
        }

        public void setRouteContext(Map<String, Map<String, Object>> routeContext) {
            this.routeContext = routeContext;
        }

        public Map<String, Object> getVarContext() {
            return varContext;
        }

        public void setVarContext(Map<String, Object> varContext) {
            this.varContext = varContext;
        }
    }

}
