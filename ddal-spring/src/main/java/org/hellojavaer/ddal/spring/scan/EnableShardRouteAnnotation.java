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
package org.hellojavaer.ddal.spring.scan;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.hellojavaer.ddal.ddr.shard.ShardRouteContext;
import org.hellojavaer.ddal.ddr.shard.annotation.ShardRoute;
import org.hellojavaer.ddal.ddr.utils.DDRStringUtils;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.core.ParameterNameDiscoverer;
import org.springframework.expression.*;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.ReflectivePropertyAccessor;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * add the following tags in spring configuration file
 * <pre>
        <aop:aspectj-autoproxy/>
        <bean class="org.hellojavaer.ddal.spring.scan.EnableShardRouteAnnotation"/>
 * </pre>
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 01/01/2017.
 */
@Aspect
@Component
public class EnableShardRouteAnnotation {

    private ParameterNameDiscoverer parameterNameDiscoverer           = null;
    private static boolean          notSupportParameterNameDiscoverer = false;

    private Map<Method, InnerBean>  expressionCache                   = new HashMap<Method, InnerBean>();

    @Around("@annotation(shardRoute)")
    public Object around(ProceedingJoinPoint joinPoint, ShardRoute shardRoute) throws Throwable {
        try {
            ShardRouteContext.pushContext();
            if (shardRoute.scName() != null && shardRoute.scName().length() > 0 //
                && shardRoute.sdValue() != null && shardRoute.sdValue().length() > 0) {
                MethodSignature methodSignature = (MethodSignature) joinPoint.getSignature();
                Method method = methodSignature.getMethod();
                InnerBean innerBean = expressionCache.get(method);
                Object[] args = joinPoint.getArgs();
                Object val = null;
                if (innerBean == null) {
                    synchronized (this) {
                        innerBean = expressionCache.get(method);
                        if (innerBean == null) {
                            ExpressionParser parser = new SpelExpressionParser();
                            Expression expression0 = null;
                            String sdValue = shardRoute.sdValue();
                            sdValue = DDRStringUtils.trimToNull(sdValue);
                            if (sdValue != null) {
                                expression0 = parser.parseExpression(sdValue, PARSER_CONTEXT);
                            }
                            String[] paramNames = getParameterNames(method);
                            val = calculate(expression0, paramNames, args);
                            InnerBean innerBean1 = new InnerBean(paramNames, expression0);
                            innerBean1.getExpression();//
                            expressionCache.put(method, innerBean1);
                        } else {
                            val = calculate(innerBean.getExpression(), innerBean.getParameterNames(), args);
                        }
                    }
                } else {
                    val = calculate(innerBean.getExpression(), innerBean.getParameterNames(), args);
                }
                String[] scNames = shardRoute.scName().split(",");
                for (String scName : scNames) {
                    ShardRouteContext.setRouteInfo(scName, val);
                }
            } else {
                if ((shardRoute.scName() == null || shardRoute.scName().length() == 0)
                    && (shardRoute.sdValue() == null || shardRoute.sdValue().length() == 0)) {
                    // ok
                } else {
                    throw new IllegalArgumentException(
                                                       "scName and sdValue should either both have a non-empty value or both have a empty value");
                }
            }
            return joinPoint.proceed(joinPoint.getArgs());
        } finally {
            ShardRouteContext.popContext();
        }
    }

    /**
     * DefaultParameterNameDiscoverer is supported spring 4.0
     * @return
     */
    private String[] getParameterNames(Method method) {
        if (notSupportParameterNameDiscoverer) {
            return null;
        } else {
            try {
                parameterNameDiscoverer = new DefaultParameterNameDiscoverer();// only support from spring4
                String[] strs = parameterNameDiscoverer.getParameterNames(method);
                if (strs == null) {
                    notSupportParameterNameDiscoverer = true;
                }
                return strs;
            } catch (NoClassDefFoundError e) {
                notSupportParameterNameDiscoverer = true;
                return null;
            }
        }
    }

    private Object calculate(Expression expression, String[] parameterNames, Object[] args) {
        if (expression == null) {
            return null;
        }
        EvaluationContext context = buildEvaluationContext();
        if (args != null && args.length > 0) {
            for (int i = 0; i < args.length; i++) {
                if (parameterNames != null && parameterNames.length > i) {
                    context.setVariable(parameterNames[i], args[i]);
                }
                context.setVariable("$" + i, args[i]);
            }
        }
        Object ret = expression.getValue(context);
        if (ret == null) {
            return null;
        } else {// FIXME: spel will return a list
            if (ret instanceof List) {
                if (((List) ret).isEmpty()) {
                    return null;
                } else {
                    return ((List) ret).get(0);
                }
            } else {
                return ret;
            }
        }
    }

    private class InnerBean {

        public InnerBean(String[] parameterNames, Expression expression) {
            this.parameterNames = parameterNames;
            this.expression = expression;
        }

        private String[]   parameterNames;
        private Expression expression;

        public String[] getParameterNames() {
            return parameterNames;
        }

        public void setParameterNames(String[] parameterNames) {
            this.parameterNames = parameterNames;
        }

        public Expression getExpression() {
            return expression;
        }

        public void setExpression(Expression expression) {
            this.expression = expression;
        }
    }

    //
    private static EvaluationContext buildEvaluationContext() {
        StandardEvaluationContext context = new StandardEvaluationContext(rootObject) {

            @Override
            public List<PropertyAccessor> getPropertyAccessors() {
                return propertyAccessors;
            }
        };
        return context;
    }

    private static final TypedValue             rootObject        = new TypedValue(null);

    private static final List<PropertyAccessor> propertyAccessors = new ArrayList<PropertyAccessor>(1);

    private static final ParserContext          PARSER_CONTEXT    = new ParserContext() {

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
    }
}
