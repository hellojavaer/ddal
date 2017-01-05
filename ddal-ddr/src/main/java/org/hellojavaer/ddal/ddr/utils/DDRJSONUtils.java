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
package org.hellojavaer.ddal.ddr.utils;

import java.util.Collection;
import java.util.Map;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 22/12/2016.
 */
public class DDRJSONUtils {

    public static String toJSONString(Collection collection) {
        if (collection == null) {
            return null;
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append('[');
            for (Object item : collection) {
                if (item == null) {
                    sb.append("null");
                } else if (item instanceof String || item instanceof Character) {
                    sb.append('\"');
                    sb.append(item);
                    sb.append('\"');
                } else if (item instanceof Map) {
                    sb.append(toJSONString((Map) item));
                } else {
                    sb.append(item);
                }
                sb.append(',');
            }
            if (!collection.isEmpty()) {
                sb.deleteCharAt(sb.length() - 1);
            }
            sb.append(']');
            return sb.toString();
        }
    }

    public static String toJSONString(Map map) {
        if (map == null) {
            return null;
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append('{');
            for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) map).entrySet()) {
                if (entry.getKey() == null) {
                    sb.append("null");
                } else if (entry.getKey() instanceof String || entry.getKey() instanceof Character) {
                    sb.append('\"');
                    sb.append(entry.getKey());
                    sb.append('\"');
                } else {
                    sb.append(entry.getKey());
                }
                sb.append(':');
                if (entry.getValue() == null) {
                    sb.append("null");
                } else if (entry.getValue() instanceof String) {
                    sb.append('\"');
                    sb.append(entry.getValue());
                    sb.append('\"');
                } else if (entry.getValue() instanceof Collection) {
                    sb.append(toJSONString((Collection) (entry.getValue())));
                } else {
                    sb.append(entry.getValue());
                }
                sb.append(',');
            }
            if (!map.isEmpty()) {
                sb.deleteCharAt(sb.length() - 1);
            }
            sb.append('}');
            return sb.toString();
        }
    }
}
