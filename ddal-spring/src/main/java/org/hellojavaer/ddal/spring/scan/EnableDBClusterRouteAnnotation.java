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

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.hellojavaer.ddal.ddr.cluster.DBClusterRoute;
import org.hellojavaer.ddal.ddr.cluster.DBClusterRouteContext;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * add the following tags in spring configuration file
 * <pre>
        <aop:aspectj-autoproxy/>
        <bean class="org.hellojavaer.ddal.spring.scan.EnableDBClusterRouteAnnotation"/>
 * </pre>
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 2018/5/27.
 */
@Aspect
@Component
public class EnableDBClusterRouteAnnotation {

    private Map<Method, MethodBasedSpelExpression> expressionCache = new HashMap<>();

    @Around("@annotation(dbClusterRoute)")
    public Object around(ProceedingJoinPoint joinPoint, DBClusterRoute dbClusterRoute) throws Throwable {
        try {
            DBClusterRouteContext.pushContext();
            Object[] args = joinPoint.getArgs();
            MethodSignature methodSignature = (MethodSignature) joinPoint.getSignature();
            Method method = methodSignature.getMethod();
            MethodBasedSpelExpression expression = expressionCache.get(method);
            if (expression == null) {
                synchronized (expressionCache) {
                    expression = expressionCache.get(method);
                    if (expression == null) {
                        expression = new MethodBasedSpelExpression(dbClusterRoute.clusterName(), method);
                        expressionCache.put(method, expression);
                    }
                }
            }
            String targetClusterName = expression.parse(String.class, args);
            DBClusterRouteContext.setClusterName(targetClusterName);
            return joinPoint.proceed(args);
        } finally {
            DBClusterRouteContext.popContext();
        }
    }
}
