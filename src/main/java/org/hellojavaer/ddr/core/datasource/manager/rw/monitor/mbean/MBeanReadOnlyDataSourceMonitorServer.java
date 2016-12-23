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
package org.hellojavaer.ddr.core.datasource.manager.rw.monitor.mbean;

import org.hellojavaer.ddr.core.datasource.manager.rw.monitor.ReadOnlyDataSourceMonitor;
import org.hellojavaer.ddr.core.datasource.manager.rw.monitor.ReadOnlyDataSourceMonitorServer;
import org.hellojavaer.ddr.core.utils.DDRStringUtils;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 23/12/2016.
 */
public class MBeanReadOnlyDataSourceMonitorServer implements ReadOnlyDataSourceMonitorServer {

    private ReadOnlyDataSourceMonitor readOnlyDataSourceMonitor;

    private String                    paramNameOfObjectName;

    public String getParamNameOfObjectName() {
        return paramNameOfObjectName;
    }

    public void setParamNameOfObjectName(String paramNameOfObjectName) {
        this.paramNameOfObjectName = DDRStringUtils.trim(paramNameOfObjectName);
    }

    @Override
    public void init(ReadOnlyDataSourceMonitor readOnlyDataSourceMonitor) {
        this.readOnlyDataSourceMonitor = readOnlyDataSourceMonitor;
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try {
            ObjectName mbeanName = null;
            if (paramNameOfObjectName == null) {
                mbeanName = new ObjectName(ReadOnlyDataSourceMonitor.class.getPackage().getName() + ":type="
                                           + ReadOnlyDataSourceMonitor.class.getSimpleName());
            } else {
                mbeanName = new ObjectName(paramNameOfObjectName);
            }
            ReadOnlyDataSourceMonitorMXBean mbean = new ReadOnlyDataSourceMonitorMXBean() {

                @Override
                public Integer getWeight(String scName, int index) {
                    return getReadOnlyDataSourceMonitor().getWeight(scName, index);
                }

                @Override
                public String setWeight(String scName, int index, int weight) {
                    return getReadOnlyDataSourceMonitor().setWeight(scName, index, weight);
                }

                @Override
                public String restoreWeight(String scName, int index) {
                    return getReadOnlyDataSourceMonitor().restoreWeight(scName, index);
                }

                @Override
                public Integer getWeight(String scName, String name) {
                    return getReadOnlyDataSourceMonitor().getWeight(scName, name);
                }

                @Override
                public String setWeight(String scName, String name, int weight) {
                    return getReadOnlyDataSourceMonitor().setWeight(scName, name, weight);
                }

                @Override
                public String restoreWeight(String scName, String name) {
                    return getReadOnlyDataSourceMonitor().restoreWeight(scName, name);
                }

                @Override
                public String getWeight(String scName) {
                    return getReadOnlyDataSourceMonitor().getWeight(scName);
                }

                @Override
                public String setWeight(String scName, String values) {
                    return getReadOnlyDataSourceMonitor().setWeight(scName, values);
                }

                @Override
                public String restoreWeight(String scName) {
                    return getReadOnlyDataSourceMonitor().restoreWeight(scName);
                }

                @Override
                public String restoreWeight() {
                    return getReadOnlyDataSourceMonitor().restoreWeight();
                }

                @Override
                public String getOriginalWeightConfig() {
                    return getReadOnlyDataSourceMonitor().getOriginalWeightConfig();
                }

                @Override
                public String getCurrentWeightConfig() {
                    return getReadOnlyDataSourceMonitor().getCurrentWeightConfig();
                }
            };
            server.registerMBean(mbean, mbeanName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected ReadOnlyDataSourceMonitor getReadOnlyDataSourceMonitor() {
        return readOnlyDataSourceMonitor;
    }
}
