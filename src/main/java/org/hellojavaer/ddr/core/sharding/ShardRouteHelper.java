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

import java.util.*;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 23/11/2016.
 */
public class ShardRouteHelper {

    private static Map<String, List>        routeMap  = new HashMap<String, List>();
    private static Map<String, Set<String>> schemaMap = new HashMap<String, Set<String>>();

    public static Set<String> getConfiguredSchemas() {
        return schemaMap.keySet();
    }

    public static Set<String> getConfiguredTables(String scName) {
        return schemaMap.get(scName);
    }

    public static void setConfiguredRouteInfos(String scName, String tbName, List<RouteInfo> routeInfos) {
        Set<String> tables = schemaMap.get(scName);
        if (tables == null) {
            synchronized (ShardRouteHelper.class) {
                tables = schemaMap.get(scName);
                if (tables == null) {
                    tables = new HashSet<String>();
                    schemaMap.put(scName, tables);
                }
            }
        }
        tables.add(tbName);
        routeMap.put(buildQueryKey(scName, tbName), routeInfos);
    }

    public static List<RouteInfo> getConfiguredRouteInfos(String scName, String tbName) {
        return routeMap.get(buildQueryKey(scName, tbName));
    }

    private static String buildQueryKey(String scName, String tbName) {
        StringBuilder sb = new StringBuilder();
        scName = DDRStringUtils.toLowerCase(scName);
        if (scName != null) {
            sb.append(scName);
            sb.append('.');
        }
        sb.append(DDRStringUtils.toLowerCase(tbName));
        return sb.toString();
    }

    public static void clear() {
        routeMap.clear();
        schemaMap.clear();
    }
}
