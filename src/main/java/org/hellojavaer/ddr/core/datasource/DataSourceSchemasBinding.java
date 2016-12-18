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
package org.hellojavaer.ddr.core.datasource;

import javax.sql.DataSource;
import java.util.Set;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 10/12/2016.
 */
public class DataSourceSchemasBinding {

    private DataSource  dataSource;
    private Set<String> schemas;

    public DataSourceSchemasBinding() {
    }

    public DataSourceSchemasBinding(DataSource dataSource, Set<String> schemas) {
        this.dataSource = dataSource;
        this.schemas = schemas;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public Set<String> getSchemas() {
        return schemas;
    }

    public void setSchemas(Set<String> schemas) {
        this.schemas = schemas;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{schemas:");
        if (schemas != null) {
            sb.append("[");
            for (String s : schemas) {
                sb.append('\'');
                sb.append(s);
                sb.append('\'');
                sb.append(',');
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append("]");
        } else {
            sb.append("null");
        }
        sb.append("}");
        return super.toString();
    }
}
