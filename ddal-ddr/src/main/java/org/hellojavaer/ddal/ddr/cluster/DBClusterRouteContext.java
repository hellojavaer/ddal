/*
 * Copyright 2018-2018 the original author or authors.
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
package org.hellojavaer.ddal.ddr.cluster;

import org.hellojavaer.ddal.ddr.expression.el.function.FormatFunction;
import org.hellojavaer.ddal.ddr.expression.el.function.MathFunction;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 2018/5/27.
 */
public class DBClusterRouteContext {

    private static final ThreadLocal<String>              clusterNameCache = new ThreadLocal();

    private static final Map<String, Object>              systemVariables  = new HashMap<>();

    private static final ThreadLocal<Map<String, Object>> variables        = new ThreadLocal() {

                                                                               @Override
                                                                               protected Map initialValue() {
                                                                                   return new HashMap();
                                                                               }
                                                                           };

    static {
        setSystemVariable(MathFunction.class);
        setSystemVariable(FormatFunction.class);
    }

    private static void setSystemVariable(Class<?> clazz) {
        Method[] methods = clazz.getDeclaredMethods();
        for (Method method : methods) {
            systemVariables.put(method.getName(), method);
        }
    }

    public static void setClusterName(String name) {
        clusterNameCache.set(name);
    }

    public static String getClusterName() {
        return clusterNameCache.get();
    }

    public static Object getVariable(String name) {
        if (name == null) {
            throw new IllegalArgumentException("name can't be null");
        }
        Object value = variables.get().get(name);
        if (value == null) {
            value = systemVariables.get(name);
        }
        return value;
    }

    public static Object setVariable(String name, Object value) {
        if (name == null) {
            throw new IllegalArgumentException("name can't be null");
        }
        if (value == null) {
            throw new IllegalArgumentException("value can't be null");
        }
        return variables.get().put(name, value);
    }

    public static Object removeVariable(String name) {
        if (name == null) {
            throw new IllegalArgumentException("name can't be null");
        }
        return variables.get().remove(name);
    }

    public static void clearContext() {
        clusterNameCache.remove();
        variables.get().clear();
    }
}
