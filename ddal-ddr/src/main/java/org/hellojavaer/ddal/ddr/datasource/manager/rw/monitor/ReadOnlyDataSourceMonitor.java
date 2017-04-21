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
package org.hellojavaer.ddal.ddr.datasource.manager.rw.monitor;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 21/12/2016.
 */
public interface ReadOnlyDataSourceMonitor {

    /**
     * 单个权重
     */
    Integer getWeight(String scName, int index);

    String setWeight(String scName, int index, int weight);

    String restoreWeight(String scName, int index);

    Integer getWeight(String scName, String dataSourceName);

    String setWeight(String scName, String dataSourceName, int weight);

    String restoreWeight(String scName, String dataSourceName);

    /**
     * 批量权重
     */
    String getWeight(String scName);// [1:{"name":"aa","desc":"bb",weight:1}]

    /**
     *  batch updating load weight of datasource may cause cluster unstable,generally I suggest you updating weight of datasource one by one in a smoothing way.
     *
     *  由于同时更新多个数据源的权重可能会导致数据库集群整体负载失衡,通常的情况下我建议平滑的升降对单个数据源的负载,因此最后我决定去掉这个方法
     */
    //String setWeight(String scName, String values);// [1:{"name":"aa","desc":"bb",weight:1}]

    String restoreWeight(String scName);//

    /**
     * 重置
     */
    String restoreWeight();

    /**
     * 读取全量配置
     */
    String getOriginalWeightConfig();

    /**
     * 读取全量配置
     */
    String getCurrentWeightConfig();
}
