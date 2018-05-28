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

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 14/12/2016.
 */
public class ShardRouteRuleExpressionContext {

    private static final Map<String, Object>              systemVariables = new HashMap<>();

    private static final ThreadLocal<LinkedList<Context>> STACK           = new ThreadLocal<LinkedList<Context>>() {

                                                                              protected LinkedList<Context> initialValue() {
                                                                                  LinkedList stack = new LinkedList<>();
                                                                                  stack.add(new Context());
                                                                                  return stack;
                                                                              }
                                                                          };

    static {
        setSystemVariable(MathFunction.class);
        setSystemVariable(FormatFunction.class);
    }

    private static void setSystemVariable(Class<?> clazz) {
        Method[] methods = clazz.getDeclaredMethods();
        for (Method method : methods) {
            setSystemVariable(method.getName(), method);
        }
    }

    private static void setSystemVariable(String name, Object value) {
        systemVariables.put(name, value);
    }

    public static Object getSystemVariable(String name) {
        return systemVariables.get(name);
    }

    public static Object getVariable(String name) {
        if (name == null) {
            throw new IllegalArgumentException("name can't be null");
        }
        for (Context context : STACK.get()) {
            Object value = context.getLocalVariables().get(name);
            if (value != null) {
                return value;
            }
        }// else
        return systemVariables.get(name);
    }

    public static Object getLocalVariable(String name) {
        if (name == null) {
            throw new IllegalArgumentException("name can't be null");
        }
        return getCurrentContext().getLocalVariables().get(name);
    }

    public static Object setLocalVariable(String name, Object value) {
        if (name == null) {
            throw new IllegalArgumentException("name can't be null");
        }
        if (value == null) {
            throw new IllegalArgumentException("value can't be null");
        }
        return getCurrentContext().getLocalVariables().put(name, value);
    }

    public static Object removeLocalVariable(String name) {
        if (name == null) {
            throw new IllegalArgumentException("name can't be null");
        }
        return getCurrentContext().getLocalVariables().remove(name);
    }

    public static void pushContext() {
        STACK.get().addFirst(new Context());
    }

    public static void popContext() throws IndexOutOfBoundsException {
        if (STACK.get().size() <= 1) {
            throw new IndexOutOfBoundsException("root context can't be pop");
        } else {
            STACK.get().removeFirst();
        }
    }

    public static void clearContext() {
        Context context = getCurrentContext();
        context.getLocalVariables().clear();
    }

    private static Context getCurrentContext() {
        return STACK.get().getFirst();
    }

    private static class Context {

        private Map<String, Object> localVariables = new HashMap<>();

        public Map<String, Object> getLocalVariables() {
            return localVariables;
        }

        public void setLocalVariables(Map<String, Object> localVariables) {
            this.localVariables = localVariables;
        }
    }
}
