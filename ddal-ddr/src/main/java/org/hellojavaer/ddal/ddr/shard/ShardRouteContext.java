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
 * 1.当路由规则中指定了分片字段时
 * 1.1 如果sql中 包含分片字段且分片字段且分片字段所属操作(in,=,between)可以用于计算分片值: 使用sql或jdbc参数带入路由规则进行路由
 * 1.2 若干sql中不包含分片字段或分片字段所属操作不能用于计算分片:使用接口getRouteInfo(scName, tbName) 获取路由信息,返回值类型必须是RouteInfo
 *
 * 2.当路由规则中未指定分片字段时
 * 调用getRouteInfo(scName, tbName)获取路由信息,返回值类型如果是RouteInfo则直接路由,如果是其他类型则使用路由规则计算路由;
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 15/11/2016.
 */
public class ShardRouteContext {

    private static final ThreadLocal<LinkedList<Context>> STACK = new ThreadLocal<LinkedList<Context>>() {

                                                                    protected LinkedList<Context> initialValue() {
                                                                        LinkedList stack = new LinkedList<Context>();
                                                                        stack.add(new Context());
                                                                        return stack;
                                                                    }
                                                                };

    //
    public static void pushContext() {
        STACK.get().addFirst(new Context());
    }

    public static void popContext() throws IndexOutOfBoundsException {
        if (STACK.get().size() <= 1) {
            throw new IndexOutOfBoundsException("root context can't be pop");
        } else {
            STACK.get().removeFirst();
        }
    }

    public static void clearContext() {
        Context context = STACK.get().getFirst();
        context.getDefaultRouteContext().clear();
        context.getExactRouteContext().clear();
        context.getAmbiguousRouteContext().clear();
    }

    //
    public static void setRouteInfo(String scName, ShardRouteInfo routeInfo) {
        setRouteInfo(scName, (Object) routeInfo);
    }

    public static void setRouteInfo(String scName, Object sdValue) {
        scName = DDRStringUtils.toLowerCase(scName);
        if (scName == null) {
            throw new IllegalArgumentException("'scName' can't be empty");
        }
        if (sdValue == null) {
            sdValue = NULL_OBJECT;
        }
        getCurContext().getDefaultRouteContext().put(scName, sdValue);
    }

    public static Object getRouteInfo(String scName) {
        scName = DDRStringUtils.toLowerCase(scName);
        if (scName == null) {
            throw new IllegalArgumentException("'scName' can't be empty");
        }
        for (Context context : STACK.get()) {
            Map<String, Object> defaultRouteContext = context.getDefaultRouteContext();
            Object val = defaultRouteContext.get(scName);
            if (val == null) {
                continue;
            } else {
                if (val == NULL_OBJECT) {
                    return null;
                } else {
                    return val;
                }
            }
        }
        return null;
    }

    public static boolean containsRouteInfo(String scName) {
        scName = DDRStringUtils.toLowerCase(scName);
        if (scName == null) {
            throw new IllegalArgumentException("'scName' can't be empty");
        }
        return getCurContext().getDefaultRouteContext().containsKey(scName);
    }

    public static Object removeRouteInfo(String scName) {
        scName = DDRStringUtils.toLowerCase(scName);
        if (scName == null) {
            throw new IllegalArgumentException("'scName' can't be empty");
        }
        return getCurContext().getDefaultRouteContext().remove(scName);
    }

    /**
     * 设置路由信息
     * 相同路由,采用覆盖原则
     */
    public static void setRouteInfo(String scName, String tbName, Object sdValue) {
        scName = DDRStringUtils.toLowerCase(scName);
        tbName = DDRStringUtils.toLowerCase(tbName);
        if (scName == null) {
            throw new IllegalArgumentException("'scName' can't be empty");
        }
        if (tbName == null) {
            throw new IllegalArgumentException("'tbName' can't be empty");
        }
        if (sdValue == null) {
            sdValue = NULL_OBJECT;
        }

        /**
         * 添加两条查询索引
         * schema_name.table_name <-> routeInfo
         * table_name <-> routeInfo
         */
        Map<String, Object> exactRouteContext = getCurContext().getExactRouteContext();
        exactRouteContext.put(buildQueryKey(scName, tbName), sdValue);
        Map<String, Map<String, Object>> ambiguousRouteContext = getCurContext().getAmbiguousRouteContext();
        Map<String, Object> scMap = ambiguousRouteContext.get(tbName);
        if (scMap == null) {
            scMap = new HashMap<>();
            ambiguousRouteContext.put(tbName, scMap);
        }
        scMap.put(scName, sdValue);
    }

    public static void setRouteInfo(String scName, String tbName, ShardRouteInfo routeInfo) {
        setRouteInfo(scName, tbName, (Object) routeInfo);
    }

    /**
     *
     * 删除路由信息
     */
    public static Object removeRouteInfo(String scName, String tbName) {
        scName = DDRStringUtils.toLowerCase(scName);
        tbName = DDRStringUtils.toLowerCase(tbName);
        if (scName == null) {
            throw new IllegalArgumentException("'scName' can't be empty");
        }
        if (tbName == null) {
            throw new IllegalArgumentException("'tbName' can't be empty");
        }
        Map<String, Object> exactRouteContext = getCurContext().getExactRouteContext();
        // level 1
        Object obj = exactRouteContext.remove(buildQueryKey(scName, tbName));
        // level 2
        Map<String, Map<String, Object>> ambiguousRouteContext = getCurContext().getAmbiguousRouteContext();
        Map<String, Object> scMap = ambiguousRouteContext.get(tbName);
        if (scMap != null) {
            scMap.remove(scName);
            if (scMap.isEmpty()) {
                ambiguousRouteContext.remove(tbName);
            }
        }
        return obj;
    }

    public static Object getRouteInfo(String scName, String tbName) throws AmbiguousDataSourceBindingException {
        scName = DDRStringUtils.toLowerCase(scName);
        tbName = DDRStringUtils.toLowerCase(tbName);
        if (tbName == null) {
            throw new IllegalArgumentException("'tbName' can't be empty");
        }
        for (Context context : STACK.get()) {
            Object object = null;
            if (scName != null) {
                object = context.getExactRouteContext().get(buildQueryKey(scName, tbName));
            } else {
                Map<String, Map<String, Object>> ambiguousRouteContext = context.getAmbiguousRouteContext();
                Map<String, Object> scMap = ambiguousRouteContext.get(tbName);
                if (scMap == null || scMap.isEmpty()) {
                    continue;
                }
                if (scMap.size() > 1) {
                    throw new AmbiguousDataSourceBindingException("Datasource binding for scName:" + scName
                                                                  + ", tbName:" + tbName + " is ambiguous");
                } else {
                    object = scMap.values().iterator().next();
                }
            }
            if (object == null) {
                continue;
            } else if (object == NULL_OBJECT) {
                return null;
            } else {
                return object;
            }
        }
        //
        if (scName == null) {
            return null;
        } else {
            // if null try to get default
            return getRouteInfo(scName);
        }
    }

    public static boolean containsRouteInfo(String scName, String tbName) {
        scName = DDRStringUtils.toLowerCase(scName);
        tbName = DDRStringUtils.toLowerCase(tbName);
        if (scName == null) {
            throw new IllegalArgumentException("'scName' can't be empty");
        }
        if (tbName == null) {
            throw new IllegalArgumentException("'tbName' can't be empty");
        }
        String fullTableName = buildQueryKey(scName, tbName);
        return getCurContext().getExactRouteContext().containsKey(fullTableName);
    }

    private static String buildQueryKey(String scName, String tbName) {
        if (scName == null) {
            return tbName;
        } else {
            return new StringBuilder().append(scName).append('.').append(tbName).toString();
        }
    }

    private static Context getCurContext() {
        return STACK.get().getFirst();
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

    private static class Context {

        private Map<String, Object>              defaultRouteContext   = new HashMap<>();
        private Map<String, Object>              exactRouteContext     = new HashMap<>();
        private Map<String, Map<String, Object>> ambiguousRouteContext = new HashMap<>();

        public Map<String, Object> getExactRouteContext() {
            return exactRouteContext;
        }

        public void setExactRouteContext(Map<String, Object> exactRouteContext) {
            this.exactRouteContext = exactRouteContext;
        }

        public Map<String, Object> getDefaultRouteContext() {
            return defaultRouteContext;
        }

        public void setDefaultRouteContext(Map<String, Object> defaultRouteContext) {
            this.defaultRouteContext = defaultRouteContext;
        }

        public Map<String, Map<String, Object>> getAmbiguousRouteContext() {
            return ambiguousRouteContext;
        }

        public void setAmbiguousRouteContext(Map<String, Map<String, Object>> ambiguousRouteContext) {
            this.ambiguousRouteContext = ambiguousRouteContext;
        }
    }

}
