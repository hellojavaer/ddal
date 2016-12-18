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

import org.hellojavaer.ddr.core.utils.DDRStringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 15/11/2016.
 */
public class ShardingRouteContext {

    private static final ThreadLocal<Map<String, Object>> ROUTE_VALUE         = new ThreadLocal<Map<String, Object>>();
    private static final ThreadLocal<Boolean>             DISABLE_SQL_ROUTING = new ThreadLocal<Boolean>();

    public static void setDisableSqlRouting(boolean disableSqlRoute) {
        if (disableSqlRoute) {
            DISABLE_SQL_ROUTING.set(Boolean.TRUE);
        }
    }

    public static boolean isDisableSqlRouting() {
        return DISABLE_SQL_ROUTING.get() == Boolean.TRUE;
    }

    /**
     * 
     * 当表scName.tbName 没有分表配置时使用该配置
     */
    public static void setRoute(String scName, String tbName, Object value) {
        Map<String, Object> map = ROUTE_VALUE.get();
        if (map == null) {
            map = new HashMap();
            ROUTE_VALUE.set(map);
        }
        map.put(buildQueryKey(scName, tbName), value);
    }

    /**
     * 设置路由信息
     * 触发条件
     * 1.sql中没有设置sdValue 时触发(如果设置了但为null不会触发),
     * 2.
     */
    public static void setRoute(String scName, String tbName, ShardingInfo value) {
        Map<String, Object> map = ROUTE_VALUE.get();
        if (map == null) {
            map = new HashMap();
            ROUTE_VALUE.set(map);
        }
        map.put(buildQueryKey(scName, tbName), value);
    }

    public static Object getRoute(String scName, String tbName) {
        Map<String, Object> map = ROUTE_VALUE.get();
        if (map == null) {
            return null;
        } else {
            return map.get(buildQueryKey(scName, tbName));
        }
    }

    private static String buildQueryKey(String scName, String tbName) {
        return new StringBuilder().append(DDRStringUtils.toLowerCase(scName))//
                                  .append('.')//
                                  .append(DDRStringUtils.toLowerCase(tbName))//
                                  .toString();//
    }

    public static void clear() {
        ROUTE_VALUE.remove();
        DISABLE_SQL_ROUTING.remove();
    }

}
