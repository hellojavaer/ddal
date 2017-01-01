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
package org.hellojavaer.ddr.core.datasource.jdbc.init;

import org.hellojavaer.ddr.core.datasource.exception.DDRDataSourceException;
import org.hellojavaer.ddr.core.datasource.jdbc.property.ConnectionProperty;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 11/12/2016.
 */
public class UninitializedConnectionProcessor {

    private static Map<ConnectionProperty, InnerBean> map = new HashMap<ConnectionProperty, InnerBean>();

    public static void setDefaultValue(ConnectionProperty prop, Object val, boolean isSyncDefaultValue) {
        if (prop == ConnectionProperty.metaData && isSyncDefaultValue) {
            throw new DDRDataSourceException("Can't execute update for metaData");
        }
        map.put(prop, new InnerBean(val, isSyncDefaultValue));
    }

    public static Object getDefaultValue(ConnectionProperty prop) {
        InnerBean obj = map.get(prop);
        if (obj != null) {
            return obj.getVal();
        } else {
            return null;
        }
    }

    public static boolean isSetDefaultValue(ConnectionProperty prop) {
        return map.containsKey(prop);
    }

    public static boolean isSyncDefaultValue(ConnectionProperty prop) {
        InnerBean obj = map.get(prop);
        if (obj != null) {
            return obj.isSyncDefaultValue();
        } else {
            return false;
        }
    }

    public static Object removeDefaultValue(ConnectionProperty prop) {
        return map.remove(prop);
    }

}
