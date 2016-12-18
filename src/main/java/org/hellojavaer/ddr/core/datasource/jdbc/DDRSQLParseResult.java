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
package org.hellojavaer.ddr.core.datasource.jdbc;

import org.hellojavaer.ddr.core.datasource.exception.CrossPreparedStatementException;

import java.util.Map;
import java.util.Set;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 17/12/2016.
 */
public class DDRSQLParseResult {

    private String      sql;
    private Set<String> schemas;
    private ParseState  parseState;

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public Set<String> getSchemas() {
        return schemas;
    }

    public void setSchemas(Set<String> schemas) {
        this.schemas = schemas;
    }

    public ParseState getParseState() {
        return parseState;
    }

    public void setParseState(ParseState parseState) {
        this.parseState = parseState;
    }

    public interface ParseState {

        void validJdbcParam(Map<Object, Object> jdbcParam) throws CrossPreparedStatementException;
    }
}
