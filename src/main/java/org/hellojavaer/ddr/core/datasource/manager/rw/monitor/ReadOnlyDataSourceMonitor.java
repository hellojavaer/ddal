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
package org.hellojavaer.ddr.core.datasource.manager.rw.monitor;

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

    Integer getWeight(String scName, String name);

    String setWeight(String scName, String name, int weight);

    String restoreWeight(String scName, String name);

    /**
     * 批量权重
     */
    String getWeight(String scName);// [1:{"name":"aa","desc":"bb",weight:1}]

    String setWeight(String scName, String values);// [1:{"name":"aa","desc":"bb",weight:1}]

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
