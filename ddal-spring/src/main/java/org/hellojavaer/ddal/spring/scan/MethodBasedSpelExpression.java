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
package org.hellojavaer.ddal.spring.scan;

import org.hellojavaer.ddal.ddr.cluster.DBClusterRouteContext;
import org.hellojavaer.ddal.ddr.expression.el.spel.DDRSpelEvaluationContext;
import org.hellojavaer.ddal.ddr.utils.DDRStringUtils;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.core.ParameterNameDiscoverer;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import java.lang.reflect.Method;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 2018/5/28.
 */
class MethodBasedSpelExpression {

    private static volatile boolean notSupportParameterNameDiscoverer = false;

    private String[]                parameterNames;
    private Expression              expression;

    public MethodBasedSpelExpression(String el, Method method) {
        el = DDRStringUtils.trimToNull(el);
        if (el != null) {
            ExpressionParser parser = new SpelExpressionParser();
            this.expression = parser.parseExpression(el, DDRSpelEvaluationContext.PARSER_CONTEXT);
            this.parameterNames = getParameterNames(method);
        }
    }

    public <T> T parse(Class<T> type, Object... args) {
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
        return expression.getValue(context, type);
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
                ParameterNameDiscoverer parameterNameDiscoverer = new DefaultParameterNameDiscoverer();// only support
                // from spring4
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

    private EvaluationContext buildEvaluationContext() {
        return new DDRSpelEvaluationContext() {

            @Override
            public Object lookupVariable(String name) {
                Object obj = null;
                try {
                    obj = super.lookupVariable(name);
                } catch (Throwable ignore) {
                }
                if (obj == null) {
                    obj = DBClusterRouteContext.lookupVariable(name);
                }
                return obj;
            }
        };
    }
}
