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

import java.sql.*;
import java.util.Map;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">zoukaiming[邹凯明]</a>,created on 20/11/2016.
 */
public abstract class StatementWrapper implements Statement {

    private Connection connection;
    private Statement  statement;

    public StatementWrapper(Connection connection, Statement statement) {
        this.connection = connection;
        this.statement = statement;
    }

    public abstract String replaceSql(String sql, Map<Integer, Object> jdbcParams);

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        sql = replaceSql(sql, null);
        return this.statement.executeQuery(sql);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        sql = replaceSql(sql, null);
        return this.statement.executeUpdate(sql);
    }

    @Override
    public void close() throws SQLException {
        this.statement.close();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return this.statement.getMaxFieldSize();
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        this.statement.setMaxFieldSize(max);
    }

    @Override
    public int getMaxRows() throws SQLException {
        return this.statement.getMaxRows();
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        this.statement.setMaxRows(max);
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        this.statement.setEscapeProcessing(enable);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return this.statement.getQueryTimeout();
    }

    @Override
    public void cancel() throws SQLException {
        this.statement.cancel();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return this.statement.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        this.statement.clearWarnings();
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        this.statement.setCursorName(name);
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        sql = replaceSql(sql, null);
        return this.statement.execute(sql);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return this.statement.getResultSet();
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return this.statement.getUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return this.statement.getMoreResults();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        this.statement.setFetchDirection(direction);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return this.statement.getFetchDirection();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        this.statement.setFetchSize(rows);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return this.statement.getFetchSize();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return this.statement.getResultSetConcurrency();
    }

    @Override
    public int getResultSetType() throws SQLException {
        return this.statement.getResultSetType();
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        sql = replaceSql(sql, null);
        this.addBatch(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        this.statement.clearBatch();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return this.statement.executeBatch();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return this.statement.getMoreResults();
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return this.statement.getGeneratedKeys();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        sql = replaceSql(sql, null);
        return statement.executeUpdate(sql, autoGeneratedKeys);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        sql = replaceSql(sql, null);
        return this.statement.executeUpdate(sql, columnIndexes);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        sql = replaceSql(sql, null);
        return this.statement.executeUpdate(sql, columnNames);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        sql = replaceSql(sql, null);
        return this.statement.execute(sql, autoGeneratedKeys);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        sql = replaceSql(sql, null);
        return this.statement.execute(sql, columnIndexes);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        sql = replaceSql(sql, null);
        return this.statement.execute(sql, columnNames);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return this.statement.getResultSetHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.statement.isClosed();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        this.statement.setPoolable(poolable);
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return this.statement.isPoolable();
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        this.statement.closeOnCompletion();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return this.statement.isCloseOnCompletion();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return this.statement.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return this.statement.isWrapperFor(iface);
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        this.statement.setQueryTimeout(seconds);
    }

}
