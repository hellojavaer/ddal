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
package org.hellojavaer.ddr.core.utils;

import java.util.Collection;
import java.util.Map;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 25/12/2016.
 */
public class DDRToStringBuilder {

    private StringBuilder sb    = new StringBuilder("{");
    private int           count = 0;

    public DDRToStringBuilder append(String name, Object val) {
        sb.append('\"');
        sb.append(name);
        sb.append("\":");
        if (val == null) {
            sb.append("null");
        } else if (val instanceof String || val instanceof Character) {
            sb.append('\"');
            sb.append(val);
            sb.append('\"');
        } else if (val instanceof Collection) {
            sb.append(DDRJSONUtils.toJSONString((Collection) val));
        } else if (val instanceof Map) {
            sb.append(DDRJSONUtils.toJSONString((Map) val));
        } else {
            sb.append(val);
        }
        sb.append(',');
        count++;
        return this;
    }

    public String toString() {
        if (count > 0) {
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append('}');
        String temp = sb.toString();
        sb.deleteCharAt(sb.length() - 1);
        if (count > 0) {
            sb.append(',');
        }
        return temp;
    }
}
