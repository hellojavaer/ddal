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
package org.hellojavaer.ddal.ddr.shard.annotation;

import org.hellojavaer.ddal.ddr.shard.enums.ContextPropagation;

import java.lang.annotation.*;

/**
 * Setting the default route information for the schema
 * <pre>
 * This configuration is fully equivalent to invoking #{org.hellojavaer.ddal.ddr.shard.ShardRouteContext.setDefaultRouteInfo},
 * and this default route configuration will take effect only in the following case:
 * 1. No 'sdKey' was specified in route rule configuration
 * 2. Specified route rule in configuration but no 'sdKey' was found in sql and no associated route information was specified by #{org.hellojavaer.ddal.ddr.shard.ShardRouteContext.setRouteInfo}
 *
 * 'ShardRoute' is designed base on 'one schema one route rule'. 'one schema one rule route' is a good practice.
 * But if you need to access one more schemas which may have different route rule in one sql, you should add this annotation on its sub-invoking method.
 *
 *</pre>
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 01/01/2017.
 */
@Target({ ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ShardRoute {

    ContextPropagation value() default ContextPropagation.CLEAR_CONTEXT;

    String scName() default "";

    String sdValue() default "";

}
