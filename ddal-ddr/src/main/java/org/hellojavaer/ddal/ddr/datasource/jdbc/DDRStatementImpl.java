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
package org.hellojavaer.ddal.ddr.datasource.jdbc;

import org.hellojavaer.ddal.ddr.datasource.exception.CrossDataSourceException;
import org.hellojavaer.ddal.ddr.datasource.exception.StatementInitializationException;
import org.hellojavaer.ddal.ddr.datasource.exception.UninitializedStatusException;
import org.hellojavaer.ddal.ddr.datasource.jdbc.init.UninitializedStatementProcessor;
import org.hellojavaer.ddal.ddr.datasource.jdbc.property.StatementProperty;
import org.hellojavaer.ddal.ddr.datasource.manager.DataSourceParam;
import org.hellojavaer.ddal.ddr.sqlparse.SQLParsedResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Set;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 20/11/2016.
 */
public abstract class DDRStatementImpl implements DDRStatement {

    private Logger        stdLogger  = LoggerFactory.getLogger("org.hellojavaer.ddr.sql");

    protected Set<String> schemas    = null;
    protected Statement   statement  = null;
    protected Connection  connection = null;
    protected boolean     readOnly   = false;

    public DDRStatementImpl(boolean readOnly, Set<String> schemas) {
        this.readOnly = readOnly;
        this.schemas = schemas;
    }

    private StatementPropertyBean prop = new StatementPropertyBean();
    private InvocationTag         tag  = new InvocationTag();

    protected boolean isCrossDataSource(Set<String> schemas) {
        if (this.schemas == null) {
            return false;
        } else {
            return !this.schemas.containsAll(schemas);
        }
    }

    private class StatementPropertyBean {

        private int     maxFieldSize;
        private int     maxRows;
        private int     fetchDirection;
        private int     fetchSize;
        private int     queryTimeout;
        private boolean escapeProcessing;
        private boolean closeOnCompletion;
        private boolean poolable;

        public int getMaxFieldSize() {
            return maxFieldSize;
        }

        public void setMaxFieldSize(int maxFieldSize) {
            this.maxFieldSize = maxFieldSize;
        }

        public int getMaxRows() {
            return maxRows;
        }

        public void setMaxRows(int maxRows) {
            this.maxRows = maxRows;
        }

        public int getFetchDirection() {
            return fetchDirection;
        }

        public void setFetchDirection(int fetchDirection) {
            this.fetchDirection = fetchDirection;
        }

        public int getFetchSize() {
            return fetchSize;
        }

        public void setFetchSize(int fetchSize) {
            this.fetchSize = fetchSize;
        }

        public int getQueryTimeout() {
            return queryTimeout;
        }

        public void setQueryTimeout(int queryTimeout) {
            this.queryTimeout = queryTimeout;
        }

        public boolean isEscapeProcessing() {
            return escapeProcessing;
        }

        public void setEscapeProcessing(boolean escapeProcessing) {
            this.escapeProcessing = escapeProcessing;
        }

        public boolean isCloseOnCompletion() {
            return closeOnCompletion;
        }

        public void setCloseOnCompletion(boolean closeOnCompletion) {
            this.closeOnCompletion = closeOnCompletion;
        }

        public boolean isPoolable() {
            return poolable;
        }

        public void setPoolable(boolean poolable) {
            this.poolable = poolable;
        }
    }

    private class InvocationTag {

        private boolean maxFieldSize;
        private boolean maxRows;
        private boolean fetchDirection;
        private boolean fetchSize;
        private boolean queryTimeout;
        private boolean escapeProcessing;
        private boolean closeOnCompletion;
        private boolean poolable;

        public boolean isMaxFieldSize() {
            return maxFieldSize;
        }

        public void setMaxFieldSize(boolean maxFieldSize) {
            this.maxFieldSize = maxFieldSize;
        }

        public boolean isMaxRows() {
            return maxRows;
        }

        public void setMaxRows(boolean maxRows) {
            this.maxRows = maxRows;
        }

        public boolean isFetchDirection() {
            return fetchDirection;
        }

        public void setFetchDirection(boolean fetchDirection) {
            this.fetchDirection = fetchDirection;
        }

        public boolean isFetchSize() {
            return fetchSize;
        }

        public void setFetchSize(boolean fetchSize) {
            this.fetchSize = fetchSize;
        }

        public boolean isQueryTimeout() {
            return queryTimeout;
        }

        public void setQueryTimeout(boolean queryTimeout) {
            this.queryTimeout = queryTimeout;
        }

        public boolean isEscapeProcessing() {
            return escapeProcessing;
        }

        public void setEscapeProcessing(boolean escapeProcessing) {
            this.escapeProcessing = escapeProcessing;
        }

        public boolean isCloseOnCompletion() {
            return closeOnCompletion;
        }

        public void setCloseOnCompletion(boolean closeOnCompletion) {
            this.closeOnCompletion = closeOnCompletion;
        }

        public boolean isPoolable() {
            return poolable;
        }

        public void setPoolable(boolean poolable) {
            this.poolable = poolable;
        }
    }

    private String initStatementAndConvertSql(String sql) throws SQLException {
        // 1. parse sql
        SQLParsedResult parsedResult = parseSql(sql, null);
        if (stdLogger.isDebugEnabled()) {
            stdLogger.debug(new StringBuilder("[ParseSql] from:")//
            .append(sql).append(" =>to: ")//
            .append(parsedResult.getSql())//
            .toString());
        }
        // 2. check if crossing datasource
        if (isCrossDataSource(parsedResult.getSchemas())) {
            throw new CrossDataSourceException("Sql '" + sql + "'");
        }
        if (statement == null) {
            DataSourceParam param = new DataSourceParam();
            param.setReadOnly(readOnly);
            param.setScNames(parsedResult.getSchemas());
            try {
                initStatementIfAbsent(param, null);
            } catch (Throwable e) {
                throw new StatementInitializationException("readOnly:" + this.readOnly + " ,SQLParsedResult:"
                                                           + parsedResult + " ,original sql:[" + sql + "]", e);
            }
            playbackInvocation(statement);
        }
        return parsedResult.getSql();
    }

    protected void initStatementIfAbsent(DataSourceParam param, String sql) throws SQLException {
        StatementWrapper statementWrapper = getStatement(param, sql);
        this.statement = statementWrapper.getStatement();
        this.connection = statementWrapper.getConnection();
        this.schemas = statementWrapper.getSchemas();
    }

    protected void playbackInvocation(Statement statement) throws SQLException {
        if (tag != null && prop != null) {
            if (tag.isEscapeProcessing()) {
                statement.setEscapeProcessing(prop.isEscapeProcessing());
            }
            if (tag.isFetchDirection()) {
                statement.setFetchDirection(prop.getFetchDirection());
            }
            if (tag.isMaxFieldSize()) {
                statement.setMaxFieldSize(prop.getMaxFieldSize());
            }
            if (tag.isFetchSize()) {
                statement.setFetchSize(prop.getFetchSize());
            }
            if (tag.isPoolable()) {
                statement.setPoolable(prop.isPoolable());
            }
            if (tag.isMaxRows()) {
                statement.setMaxRows(prop.getMaxRows());
            }
            if (tag.isQueryTimeout()) {
                statement.setQueryTimeout(prop.getQueryTimeout());
            }
            if (tag.isCloseOnCompletion()) {
                if (prop.isCloseOnCompletion()) {
                    statement.closeOnCompletion();
                }
            }
        }
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        sql = initStatementAndConvertSql(sql);
        return statement.execute(sql);
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        sql = initStatementAndConvertSql(sql);
        return statement.executeQuery(sql);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        sql = initStatementAndConvertSql(sql);
        return statement.executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        sql = initStatementAndConvertSql(sql);
        return statement.executeUpdate(sql, columnNames);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        sql = initStatementAndConvertSql(sql);
        return statement.execute(sql, autoGeneratedKeys);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        sql = initStatementAndConvertSql(sql);
        return statement.execute(sql, columnIndexes);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        sql = initStatementAndConvertSql(sql);
        return statement.execute(sql, columnNames);
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        sql = initStatementAndConvertSql(sql);
        return statement.executeUpdate(sql, autoGeneratedKeys);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        sql = initStatementAndConvertSql(sql);
        return statement.executeUpdate(sql, columnIndexes);
    }

    // batch
    @Override
    public void addBatch(String sql) throws SQLException {
        sql = initStatementAndConvertSql(sql);
        statement.addBatch(sql);
    }

    // 灵活处理方案
    @Override
    public void clearBatch() throws SQLException {
        if (statement != null) {
            statement.clearBatch();
        } else {
            // ignore;
        }
    }

    @Override
    public int[] executeBatch() throws SQLException {
        if (statement != null) {
            return statement.executeBatch();
        } else {
            return new int[0];
        }
    }

    // 未初始化前可以调用的方法
    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        if (statement != null) {
            statement.setMaxFieldSize(max);
        } else {
            tag.setMaxFieldSize(true);
            prop.setMaxFieldSize(max);
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        if (statement != null) {
            return statement.getMaxFieldSize();
        } else if (tag.isMaxFieldSize()) {
            return prop.getMaxFieldSize();
        } else {
            if (UninitializedStatementProcessor.isSetDefaultValue(StatementProperty.maxFieldSize)) {
                int val = ((Number) UninitializedStatementProcessor.getDefaultValue(StatementProperty.maxFieldSize)).intValue();
                if (UninitializedStatementProcessor.isSyncDefaultValue(StatementProperty.maxFieldSize)) {
                    prop.setMaxFieldSize(val);
                    tag.setMaxFieldSize(true);
                }
                return val;
            } else {
                throw new UninitializedStatusException(
                                                       "Can't invoke 'getMaxFieldSize()' before 'setMaxFieldSize(int max)' is invoked or statement is initialized");
            }
        }
    }

    @Override
    public int getMaxRows() throws SQLException {
        if (statement != null) {
            return statement.getMaxRows();
        } else if (tag.isMaxRows()) {
            return prop.getMaxRows();
        } else {
            if (UninitializedStatementProcessor.isSetDefaultValue(StatementProperty.maxRows)) {
                int val = ((Number) UninitializedStatementProcessor.getDefaultValue(StatementProperty.maxRows)).intValue();
                if (UninitializedStatementProcessor.isSyncDefaultValue(StatementProperty.maxRows)) {
                    prop.setMaxRows(val);
                    tag.setMaxRows(true);
                }
                return val;
            } else {
                throw new UninitializedStatusException(
                                                       "Can't invoke 'getMaxRows()' before 'setMaxRows' is invoked or statement is initialized");
            }
        }
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        if (statement != null) {
            statement.setMaxRows(max);
        } else {
            tag.setMaxRows(true);
            prop.setMaxRows(max);
        }
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (statement != null) {
            statement.setFetchDirection(direction);
        } else {
            tag.setFetchDirection(true);
            prop.setFetchDirection(direction);
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        if (statement != null) {
            return statement.getFetchDirection();
        } else if (tag.isFetchDirection()) {
            return prop.getFetchDirection();
        } else {
            if (UninitializedStatementProcessor.isSetDefaultValue(StatementProperty.fetchDirection)) {
                int val = ((Number) UninitializedStatementProcessor.getDefaultValue(StatementProperty.fetchDirection)).intValue();
                if (UninitializedStatementProcessor.isSyncDefaultValue(StatementProperty.fetchDirection)) {
                    prop.setFetchDirection(val);
                    tag.setFetchDirection(true);
                }
                return val;
            } else {
                throw new UninitializedStatusException(
                                                       "Can't invoke 'getFetchDirection()' before 'setFetchDirection(int direction)' is invoked or statement is initialized");
            }
        }
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        if (statement != null) {
            statement.setFetchSize(rows);
        } else {
            tag.setFetchSize(true);
            prop.setFetchSize(rows);
        }
    }

    @Override
    public int getFetchSize() throws SQLException {
        if (statement != null) {
            return statement.getFetchSize();
        } else if (tag.isFetchSize()) {
            return prop.getFetchSize();
        } else {
            if (UninitializedStatementProcessor.isSetDefaultValue(StatementProperty.fetchSize)) {
                int val = ((Number) UninitializedStatementProcessor.getDefaultValue(StatementProperty.fetchSize)).intValue();
                if (UninitializedStatementProcessor.isSyncDefaultValue(StatementProperty.fetchSize)) {
                    prop.setFetchSize(val);
                    tag.setFetchSize(true);
                }
                return val;
            } else {
                throw new UninitializedStatusException(
                                                       "Can't invoke 'getFetchSize()' before 'setFetchSize(int rows)' is invoked or statement is initialized");
            }
        }
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        if (statement != null) {
            statement.setQueryTimeout(seconds);
        } else {
            tag.setQueryTimeout(true);
            prop.setQueryTimeout(seconds);
        }
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        if (statement != null) {
            return statement.getQueryTimeout();
        } else if (tag.isQueryTimeout()) {
            return prop.getQueryTimeout();
        } else {
            if (UninitializedStatementProcessor.isSetDefaultValue(StatementProperty.queryTimeout)) {
                int val = ((Number) UninitializedStatementProcessor.getDefaultValue(StatementProperty.queryTimeout)).intValue();
                if (UninitializedStatementProcessor.isSyncDefaultValue(StatementProperty.queryTimeout)) {
                    prop.setQueryTimeout(val);
                    tag.setQueryTimeout(true);
                }
                return val;
            } else {
                throw new UninitializedStatusException(
                                                       "Can't invoke 'getQueryTimeout()' before 'setQueryTimeout(int seconds)' is invoked or statement is initialized");
            }
        }
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        if (statement != null) {
            statement.closeOnCompletion();
        } else {
            tag.setCloseOnCompletion(true);
            prop.setCloseOnCompletion(true);
        }
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        if (statement != null) {
            return statement.isCloseOnCompletion();
        } else if (tag.isCloseOnCompletion()) {
            return prop.isCloseOnCompletion();
        } else {
            if (UninitializedStatementProcessor.isSetDefaultValue(StatementProperty.closeOnCompletion)) {
                boolean val = (boolean) UninitializedStatementProcessor.getDefaultValue(StatementProperty.closeOnCompletion);
                if (UninitializedStatementProcessor.isSyncDefaultValue(StatementProperty.closeOnCompletion)) {
                    prop.setCloseOnCompletion(true);
                    tag.setCloseOnCompletion(true);
                }
                return val;
            } else {
                throw new UninitializedStatusException(
                                                       "Can't invoke 'isCloseOnCompletion()' before 'closeOnCompletion()' is invoked or statement is initialized");
            }
        }
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        if (statement != null) {
            statement.setPoolable(poolable);
        } else {
            tag.setPoolable(true);
            prop.setPoolable(poolable);
        }
    }

    @Override
    public boolean isPoolable() throws SQLException {
        if (statement != null) {
            return statement.isPoolable();
        } else if (tag.isPoolable()) {
            return prop.isPoolable();
        } else {
            if (UninitializedStatementProcessor.isSetDefaultValue(StatementProperty.poolable)) {
                boolean val = ((boolean) UninitializedStatementProcessor.getDefaultValue(StatementProperty.poolable));
                if (UninitializedStatementProcessor.isSyncDefaultValue(StatementProperty.poolable)) {
                    prop.setPoolable(val);
                    tag.setPoolable(true);
                }
                return val;
            } else {
                throw new UninitializedStatusException(
                                                       "Can't invoke 'isPoolable()' before 'setPoolable(boolean poolable)' is invoked or statement is initialized");
            }
        }
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        if (statement != null) {
            statement.setEscapeProcessing(enable);
        } else {
            tag.setEscapeProcessing(true);
            prop.setEscapeProcessing(enable);
        }
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        if (statement != null) {// TODO
            statement.setCursorName(name);
        } else {
            throw new UninitializedStatusException("Can't invoke 'setCursorName()' before statement is initialized");
        }
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        if (statement != null) {
            return statement.getGeneratedKeys();
        } else {
            throw new UninitializedStatusException("Can't invoke 'getGeneratedKeys()' before statement is initialized");
        }
    }

    // 弹性处理的方法
    @Override
    public boolean isClosed() throws SQLException {
        if (statement != null) {
            return this.statement.isClosed();
        } else {
            return false;
        }
    }

    @Override
    public void close() throws SQLException {
        if (statement != null) {
            statement.close();
        } else {// TODO
            // ignore;
        }
    }

    @Override
    public void cancel() throws SQLException {
        if (statement != null) {
            statement.cancel();
        } else {// TODO
            // ignore;
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        if (connection != null) {
            return this.connection;
        } else {
            throw new UninitializedStatusException("Can't invoke 'getConnection()' before statement is initialized");
        }
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        if (statement != null) {
            return statement.getMoreResults(current);
        } else {
            throw new UninitializedStatusException(
                                                   "Can't invoke 'getMoreResults(int current)' before statement is initialized");
        }
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        if (statement != null) {
            return statement.getMoreResults();
        } else {
            throw new UninitializedStatusException("Can't invoke 'getMoreResults()' before statement is initialized");
        }
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        if (statement != null) {
            return statement.getResultSetHoldability();
        } else {
            throw new UninitializedStatusException(
                                                   "Can't invoke 'getResultSetHoldability()' before statement is initialized");
        }
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        if (statement != null) {
            return statement.getResultSet();
        } else {
            throw new UninitializedStatusException("Can't invoke 'getResultSet()' before statement is initialized");
        }
    }

    @Override
    public int getUpdateCount() throws SQLException {
        if (statement != null) {
            return statement.getUpdateCount();
        } else {
            throw new UninitializedStatusException("Can't invoke 'getUpdateCount()' before statement is initialized");
        }
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        if (statement != null) {
            return statement.getResultSetConcurrency();
        } else {
            throw new UninitializedStatusException(
                                                   "Can't invoke 'getResultSetConcurrency()' before statement is initialized");
        }
    }

    @Override
    public int getResultSetType() throws SQLException {
        if (statement != null) {
            return statement.getResultSetConcurrency();
        } else {
            throw new UninitializedStatusException("Can't invoke 'getResultSetType()' before statement is initialized");
        }
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        if (statement != null) {
            return statement.getWarnings();
        } else {
            throw new UninitializedStatusException("Can't invoke 'getWarnings()' before statement is initialized");
        }
    }

    @Override
    public void clearWarnings() throws SQLException {
        if (statement != null) {
            statement.clearWarnings();
        } else {
            throw new UninitializedStatusException("Can't invoke 'clearWarnings()' before statement is initialized");
        }
    }

    //
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return (T) this;
        }
        throw new SQLException("DataSource of type [" + getClass().getName() + "] cannot be unwrapped as ["
                               + iface.getName() + "]");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

}
