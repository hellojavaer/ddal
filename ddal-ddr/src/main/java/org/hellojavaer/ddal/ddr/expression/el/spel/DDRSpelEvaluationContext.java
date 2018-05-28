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
package org.hellojavaer.ddal.ddr.expression.el.spel;

import org.springframework.core.MethodParameter;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.expression.*;
import org.springframework.expression.spel.support.ReflectionHelper;
import org.springframework.expression.spel.support.ReflectiveMethodResolver;
import org.springframework.expression.spel.support.ReflectivePropertyAccessor;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 2018/5/28.
 */
public class DDRSpelEvaluationContext extends StandardEvaluationContext {

    private static final TypedValue             rootObject        = new TypedValue(null);

    private static final List<PropertyAccessor> propertyAccessors = new ArrayList<PropertyAccessor>(1);

    private static final List<MethodResolver>   methodResolvers   = new ArrayList<>(2);

    public static final ParserContext           PARSER_CONTEXT    = new ParserContext() {

                                                                      public boolean isTemplate() {
                                                                          return true;
                                                                      }

                                                                      public String getExpressionPrefix() {
                                                                          return "{";
                                                                      }

                                                                      public String getExpressionSuffix() {
                                                                          return "}";
                                                                      }
                                                                  };

    static {
        //
        propertyAccessors.add(new ReflectivePropertyAccessor());
        propertyAccessors.add(new PropertyAccessor() {

            public Class<?>[] getSpecificTargetClasses() {
                return null;
            }

            public boolean canRead(EvaluationContext context, Object target, String name) throws AccessException {
                return true;
            }

            public TypedValue read(EvaluationContext context, Object target, String name) throws AccessException {
                return new TypedValue(context.lookupVariable(name));
            }

            public boolean canWrite(EvaluationContext context, Object target, String name) throws AccessException {
                return false;
            }

            public void write(EvaluationContext context, Object target, String name, Object newValue)
                                                                                                     throws AccessException {

            }
        });
        //
        methodResolvers.add(new ReflectiveMethodResolver());
        methodResolvers.add(new MethodResolver() {

            @Override
            public MethodExecutor resolve(EvaluationContext context, Object targetObject, final String name,
                                          List<TypeDescriptor> argumentTypes) throws AccessException {
                final Method method = (Method) context.lookupVariable(name);
                if (method == null) {
                    return null;
                }
                return new MethodExecutor() {

                    @Override
                    public TypedValue execute(EvaluationContext context, Object target, Object... arguments)
                                                                                                            throws AccessException {
                        try {
                            if (method.isVarArgs()) {
                                arguments = ReflectionHelper.setupArgumentsForVarargsInvocation(method.getParameterTypes(),
                                                                                                arguments);
                            }
                            ReflectionUtils.makeAccessible(method);
                            Object value = method.invoke(target, arguments);
                            return new TypedValue(value,
                                                  new TypeDescriptor(new MethodParameter(method, -1)).narrow(value));
                        } catch (Exception ex) {
                            throw new AccessException("Problem invoking method: " + method, ex);
                        }
                    }
                };
            }
        });
    }

    public DDRSpelEvaluationContext() {
        super(rootObject);
    }

    @Override
    public List<PropertyAccessor> getPropertyAccessors() {
        return propertyAccessors;
    }

    @Override
    public List<MethodResolver> getMethodResolvers() {
        return methodResolvers;
    }

}
