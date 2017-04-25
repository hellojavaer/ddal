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
package org.hellojavaer.ddal.ddr.expression.el.function;

import org.hellojavaer.ddal.ddr.utils.DDRStringUtils;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 14/12/2016.
 */
public class ELFunctionManager {

    private static Map<String, Method> map = new ConcurrentHashMap<String, Method>(128);

    static {
        registerFunction(MathFunction.class);
        registerFunction(FormatFunction.class);
    }

    private static void registerFunction(Class<?> clazz) {
        Method[] methods = clazz.getDeclaredMethods();
        for (Method method : methods) {
            ELFunctionManager.registerFunction(method.getName(), method);
        }
    }

    public static void registerFunction(String functionName, Method method) {
        functionName = DDRStringUtils.trimToNull(functionName);
        if (functionName == null) {
            throw new IllegalArgumentException("function name can't be empty");
        }
        map.put(functionName, method);
    }

    public static Method unregisterFunction(String functionName) {
        functionName = functionName.trim();
        if (functionName.length() == 0) {
            throw new IllegalArgumentException("function name can't be empty");
        }
        return map.remove(functionName);
    }

    public static Method getRegisteredFunction(String name) {
        return map.get(name);
    }

    public static Set<Map.Entry<String, Method>> getRegisteredFunctions() {
        return map.entrySet();
    }

    public static void reset() {
        map.clear();
    }
}
