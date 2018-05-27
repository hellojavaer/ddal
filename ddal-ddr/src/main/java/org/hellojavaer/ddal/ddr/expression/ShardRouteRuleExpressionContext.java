/*
 * Copyright 2016-2018 the original author or authors.
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
package org.hellojavaer.ddal.ddr.expression;

import org.hellojavaer.ddal.ddr.expression.el.function.FormatFunction;
import org.hellojavaer.ddal.ddr.expression.el.function.MathFunction;
import org.hellojavaer.ddal.ddr.utils.DDRStringUtils;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 14/12/2016.
 */
public class ShardRouteRuleExpressionContext {

    private static ConcurrentHashMap<String, Object> map = new ConcurrentHashMap<>(128);

    static {
        init();
    }

    private static void init() {
        registerMethodVariable(MathFunction.class);
        registerMethodVariable(FormatFunction.class);
    }

    private static void registerMethodVariable(Class<?> clazz) {
        Method[] methods = clazz.getDeclaredMethods();
        for (Method method : methods) {
            ShardRouteRuleExpressionContext.registerVariable(method.getName(), method);
        }
    }

    public static void registerVariable(String variableName, Object value) {
        variableName = DDRStringUtils.trimToNull(variableName);
        if (variableName == null) {
            throw new IllegalArgumentException("variableName can't be empty");
        }
        if (value == null) {
            throw new IllegalArgumentException("value can't be null");
        }
        if (map.putIfAbsent(variableName, value) != null) {
            throw new IllegalStateException("Duplicate register variable:" + variableName);
        }
    }

    public static Object unregisterVariable(String variableName) {
        variableName = variableName.trim();
        if (variableName.length() == 0) {
            throw new IllegalArgumentException("variableName can't be empty");
        }
        return map.remove(variableName);
    }

    public static Object getVariable(String name) {
        return map.get(name);
    }

    public static Set<Map.Entry<String, Object>> getVariables() {
        return map.entrySet();
    }

    public static void reset() {
        map.clear();
        init();
    }
}
