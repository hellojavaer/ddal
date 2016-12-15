/*
 * Copyright 2016-2016 the original author or authors.
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
package org.hellojavaer.ddr.core.sharding;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">zoukaiming[邹凯明]</a>,created on 05/11/2016.
 */
public interface ShardingRouter {

    class SqlStatementType {

        public static final int INSERT = 0;

        public static final int DELETE = 1;

        public static final int UPDATE = 2;

        public static final int SELECT = 3;

    }

    void beginExecution(ShardingRouteParamContext context);

    void endExecution(ShardingRouteParamContext context);

    void beginStatement(ShardingRouteParamContext context, int sqlStatementType);

    void endStatement(ShardingRouteParamContext context);

    void beginSubSelect(ShardingRouteParamContext context);

    void endSubSelect(ShardingRouteParamContext context);

    boolean isRoute(ShardingRouteParamContext context, String scName, String tbName);

    String getRouteColName(ShardingRouteParamContext context, String scName, String tbName);

    ShardingInfo route(ShardingRouteParamContext context, String scName, String tbName, Object sdValue);

}
