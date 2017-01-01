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
package org.hellojavaer.ddr.core.datasource.manager.rw;

import org.hellojavaer.ddr.core.datasource.manager.DataSourceManager;
import org.hellojavaer.ddr.core.datasource.manager.rw.monitor.ReadOnlyDataSourceMonitor;
import org.hellojavaer.ddr.core.datasource.manager.rw.monitor.ReadOnlyDataSourceMonitorServer;

import java.util.List;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 19/11/2016.
 */
public interface ReadWriteDataSourceManager extends DataSourceManager {

    List<WriteOnlyDataSourceBinding> getWriteOnlyDataSources();

    List<ReadOnlyDataSourceBinding> getReadOnlyDataSources();

    ReadOnlyDataSourceMonitor getReadOnlyDataSourceMonitor();

    ReadOnlyDataSourceMonitorServer getReadOnlyDataSourceMonitorServer();

}
