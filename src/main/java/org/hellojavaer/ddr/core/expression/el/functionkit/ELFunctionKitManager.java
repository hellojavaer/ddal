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
package org.hellojavaer.ddr.core.expression.el.functionkit;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 14/12/2016.
 */
public class ELFunctionKitManager {

    private static Map<String, Method> map = new HashMap<String, Method>();

    public static void registerFunction(String functionName, Method method) {
        functionName = functionName.trim();
        if (functionName.length() == 0) {
            throw new IllegalArgumentException("function name can't be empty");
        }
        if (map.containsKey(functionName)) {
            throw new IllegalArgumentException("function mame '" + functionName + "' already exist");
        } else {
            map.put(functionName, method);
        }
    }

    public static Method unregisterFunction(String functionName) {
        functionName = functionName.trim();
        if (functionName.length() == 0) {
            throw new IllegalArgumentException("function name can't be empty");
        }
        return map.remove(functionName);
    }

    public static Set<Map.Entry<String, Method>> getRegisteredFunctions() {
        return map.entrySet();
    }

}
