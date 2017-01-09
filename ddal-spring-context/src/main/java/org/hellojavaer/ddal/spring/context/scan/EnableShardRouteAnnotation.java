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
package org.hellojavaer.ddal.spring.context.scan;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.hellojavaer.ddal.ddr.shard.ShardRouteContext;
import org.hellojavaer.ddal.ddr.shard.annotation.ShardRoute;
import org.hellojavaer.ddal.ddr.shard.enums.ContextPropagation;
import org.hellojavaer.ddal.ddr.shard.enums.DisableSqlRouting;
import org.hellojavaer.ddal.ddr.utils.DDRStringUtils;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.core.ParameterNameDiscoverer;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.ParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 01/01/2017.
 */
@Aspect
@Component
public class EnableShardRouteAnnotation {

    private ParameterNameDiscoverer parameterNameDiscoverer           = new DefaultParameterNameDiscoverer();
    private static boolean          notSupportParameterNameDiscoverer = false;

    private Map<Method, InnerBean>  expressionCache                   = new HashMap<Method, InnerBean>();

    @Around("@annotation(shardRoute)")
    public Object around(ProceedingJoinPoint joinPoint, ShardRoute shardRoute) throws Throwable {
        try {
            //
            if (shardRoute.value() == ContextPropagation.SUB_CONTEXT) {
                ShardRouteContext.pushSubContext();
            } else {
                ShardRouteContext.clear();
            }
            if (shardRoute.disableSqlRouting() == DisableSqlRouting.TRUE) {
                ShardRouteContext.setDisableSqlRouting(Boolean.TRUE);
            } else if (shardRoute.disableSqlRouting() == DisableSqlRouting.FALSE) {
                ShardRouteContext.setDisableSqlRouting(Boolean.FALSE);
            }
            if (shardRoute.sdKey() != null && shardRoute.sdKey().length() > 0 //
                && shardRoute.sdValue() != null && shardRoute.sdValue().length() > 0) {// 禁止对系统变量路由
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
                            sdValue = DDRStringUtils.trim(sdValue);
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
                ShardRouteContext.setParameter(shardRoute.sdKey(), val);
            } else {
                if ((shardRoute.sdKey() == null || shardRoute.sdKey().length() == 0)
                    && (shardRoute.sdValue() == null || shardRoute.sdValue().length() == 0)) {
                    // ok
                } else {
                    throw new IllegalArgumentException(
                                                       "sdKey and sdValue should either both have a non-empty value or both have a empty value");
                }
            }
            return joinPoint.proceed(joinPoint.getArgs());
        } finally {
            if (shardRoute.value() == ContextPropagation.SUB_CONTEXT) {
                ShardRouteContext.popSubContext();
            } else {
                ShardRouteContext.clear();
            }
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
                return parameterNameDiscoverer.getParameterNames(method);
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
        EvaluationContext context = new StandardEvaluationContext();
        if (args != null && args.length > 0) {
            for (int i = 0; i < args.length; i++) {
                if (parameterNames != null && parameterNames.length > i) {
                    context.setVariable(parameterNames[i], args[i]);
                }
                context.setVariable("$" + i, args[i]);
            }
        }
        return expression.getValue(context);
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

    private static ParserContext PARSER_CONTEXT = new ParserContext() {

                                                    public boolean isTemplate() {
                                                        return false;
                                                    }

                                                    public String getExpressionPrefix() {
                                                        return null;
                                                    }

                                                    public String getExpressionSuffix() {
                                                        return null;
                                                    }
                                                };
}
