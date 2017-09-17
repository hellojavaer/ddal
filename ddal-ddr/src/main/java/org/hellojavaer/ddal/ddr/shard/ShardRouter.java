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
package org.hellojavaer.ddal.ddr.shard;

import org.hellojavaer.ddal.ddr.shard.exception.ShardRouteException;
import org.hellojavaer.ddal.ddr.shard.exception.ShardValueNotFoundException;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * All the shard route ways designed in 'DDR' can be summarized as two ways.
 * <pre>
 * The one: one table one shard value.
 *    this case can't used to to divide data which don't have strongly relevance. the great advantages of this way is you can use aggregate function in a ordinary way.
 *    But as the limit you may lose some load balancing for reading and writing.
 *
 * The other: one table multiple shard values.
 *   this dividing way can implement load balancing for reading and writing. But as the limit you can't use aggregate function in a ordinary way.
 *
 * </pre>
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 05/11/2016.
 */
public interface ShardRouter {

    ShardRouteRule getRouteRule(String scName, String tbName);

    ShardRouteConfig getRouteConfig(String scName, String tbName);

    ShardRouteInfo getRouteInfo(String scName, String tbName, Object sdValue) throws ShardValueNotFoundException,
                                                                                     ShardRouteException;

    List<ShardRouteInfo> getRouteInfos(String scName, String tbName);

    Map<String, Set<String>> getRoutedTables();
}
