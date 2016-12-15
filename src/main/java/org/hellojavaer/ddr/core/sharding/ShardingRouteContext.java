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
 * @author <a href="mailto:hellojavaer@gmail.com">zoukaiming[邹凯明]</a>,created on 15/11/2016.
 */
public class ShardingRouteContext {

    private static final ThreadLocal<Map> routeValue = new ThreadLocal();

    public static void setRoute(String scName, String tbName, Long value) {
        Map<String, Object> map = routeValue.get();
        if (map == null) {
            map = new HashMap();
            routeValue.set(map);
        }
        map.put(buildQueryKey(scName, tbName), value);
    }

    public static void setRoute(String scName, String tbName, ShardingInfo value) {
        Map<String, Object> map = routeValue.get();
        if (map == null) {
            map = new HashMap();
            routeValue.set(map);
        }
        map.put(buildQueryKey(scName, tbName), value);
    }

    public static Object getRoute(String scName, String tbName) {
        Map<String, Long> map = routeValue.get();
        if (map == null) {
            return null;
        } else {
            return map.get(buildQueryKey(scName, tbName));
        }
    }

    private static String buildQueryKey(String scName, String tbName) {
        return new StringBuilder().append(DDRStringUtils.trim(scName).toLowerCase())//
                                  .append('.')//
                                  .append(DDRStringUtils.trim(tbName).toLowerCase())//
                                  .toString();//
    }

    public static void clear() {
        routeValue.remove();
    }

}
