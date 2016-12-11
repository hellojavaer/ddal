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

import org.hellojavaer.ddr.core.datasource.manager.DataSourceParam;
import org.hellojavaer.ddr.core.exception.DDRException;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">zoukaiming[邹凯明]</a>,created on 20/11/2016.
 */
public abstract class PreparedStatementWrapper implements PreparedStatement {

    private PreparedStatement    statement;
    private String               sql;
    private Map<Integer, Object> jdbcParameterForFirstAddBatch = new HashMap<Integer, Object>();
    private Map<Integer, Object> jdbcParameter                 = new HashMap<Integer, Object>();
    private boolean              readOnly;

    public abstract DDRDataSource.ReplacedResult replaceSql(String sql, Map<Integer, Object> jdbcParams)
                                                                                                        throws SQLException;

    public abstract PreparedStatement getStatement(DataSourceParam param, String routedSql) throws SQLException;

    public abstract boolean isCrossDataSource(Set<String> schemas);

    public PreparedStatementWrapper(String sql, boolean readOnly) {
        this.sql = sql;
        this.readOnly = readOnly;
        this.pushNewExecutionContext();
    }

    private List<ExecutionContext> executionContexts = new LinkedList<ExecutionContext>();

    private ExecutionContext getCurParamContext() {
        return executionContexts.get(executionContexts.size() - 1);
    }

    private void pushNewExecutionContext() {
        ExecutionContext newContext = new ExecutionContext();
        newContext.setInvokeRecords(new ArrayList<InvokeRecord>());
        executionContexts.add(newContext);
    }

    private void pushNewExecutionContext(String sql) {
        ExecutionContext newContext = new ExecutionContext();
        newContext.setStatementBatchSql(sql);
        executionContexts.add(newContext);
    }

    private class ExecutionContext {

        private List<InvokeRecord> invokeRecords;
        private String             statementBatchSql;

        public List<InvokeRecord> getInvokeRecords() {
            return invokeRecords;
        }

        public void setInvokeRecords(List<InvokeRecord> invokeRecords) {
            this.invokeRecords = invokeRecords;
        }

        public String getStatementBatchSql() {
            return statementBatchSql;
        }

        public void setStatementBatchSql(String statementBatchSql) {
            this.statementBatchSql = statementBatchSql;
        }
    }

    private class InvokeRecord {

        private String   methodName;
        private Object[] params;
        private Class[]  paramTypes;

        public InvokeRecord(String methodName, Object[] params, Class[] paramTypes) {
            this.methodName = methodName;
            this.params = params;
            this.paramTypes = paramTypes;
        }

        public String getMethodName() {
            return methodName;
        }

        public void setMethodName(String methodName) {
            this.methodName = methodName;
        }

        public Object[] getParams() {
            return params;
        }

        public void setParams(Object[] params) {
            this.params = params;
        }

        public Class[] getParamTypes() {
            return paramTypes;
        }

        public void setParamTypes(Class[] paramTypes) {
            this.paramTypes = paramTypes;
        }
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        playbackInvocation(this.sql);
        return this.statement.executeQuery();
    }

    @Override
    public int executeUpdate() throws SQLException {
        playbackInvocation(this.sql);
        return this.statement.executeUpdate();
    }

    @Override
    public void clearParameters() throws SQLException {
        this.jdbcParameterForFirstAddBatch.clear();
        this.jdbcParameter.clear();
        Iterator<ExecutionContext> it = executionContexts.iterator();
        while (it.hasNext()) {
            ExecutionContext context = it.next();
            if (context.getStatementBatchSql() == null) {
                it.remove();
            }
        }
    }

    @Override
    public boolean execute() throws SQLException {
        this.playbackInvocation(this.sql);
        return this.statement.execute();
    }

    @Override
    public void addBatch() throws SQLException {
        this.pushNewExecutionContext();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw new UnsupportedOperationException("getMetaData");
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new UnsupportedOperationException("getParameterMetaData");
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        sql = this.playbackInvocation(sql);
        return this.statement.executeQuery(sql);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        sql = this.playbackInvocation(sql);
        return this.statement.executeUpdate(sql);
    }

    @Override
    public void close() throws SQLException {
        this.statement.close();
    }

    private StatementProperty statementProperty = new StatementProperty();

    private class StatementProperty {

        private int maxFieldSize;
        private int maxRows;
        private int fetchDirection;
        private int fetchSize;

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
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        if (statement == null) {
            return statementProperty.getMaxFieldSize();
        } else {
            return this.statement.getMaxFieldSize();
        }
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        if (statement == null) {
            statementProperty.setMaxFieldSize(max);
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setMaxFieldSize", new Object[] { max },
                                                                              new Class[] { int.class }));
        } else {
            this.statement.setMaxFieldSize(max);
        }
    }

    @Override
    public int getMaxRows() throws SQLException {
        if (statement == null) {
            return statementProperty.getMaxRows();
        } else {
            return this.statement.getMaxRows();
        }
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        if (statement == null) {
            statementProperty.setMaxRows(max);
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setMaxRows", new Object[] { max },
                                                                              new Class[] { int.class }));
        } else {
            this.statement.setMaxRows(max);
        }
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (statement == null) {
            statementProperty.setFetchDirection(direction);
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setFetchDirection",
                                                                              new Object[] { direction },
                                                                              new Class[] { int.class }));
        } else {
            this.statement.setFetchDirection(direction);
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        if (statement == null) {
            return statementProperty.getFetchDirection();
        } else {
            return this.statement.getFetchDirection();
        }
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        if (statement == null) {
            statementProperty.setFetchSize(rows);
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setFetchSize", new Object[] { rows },
                                                                              new Class[] { int.class }));
        } else {
            this.statement.setFetchSize(rows);
        }
    }

    @Override
    public int getFetchSize() throws SQLException {
        if (statement == null) {
            return statementProperty.getFetchSize();
        } else {
            return this.statement.getFetchSize();
        }
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setEscapeProcessing",
                                                                              new Object[] { enable },
                                                                              new Class[] { boolean.class }));
        } else {
            this.statement.setEscapeProcessing(enable);
        }
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
        sql = this.playbackInvocation(sql);
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
    public int getResultSetConcurrency() throws SQLException {
        return this.statement.getResultSetConcurrency();
    }

    @Override
    public int getResultSetType() throws SQLException {
        return this.statement.getResultSetType();
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        this.pushNewExecutionContext(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        this.executionContexts.clear();
        this.pushNewExecutionContext();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        this.playbackInvocation(this.sql);
        return this.statement.executeBatch();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.statement.getConnection();// TODO
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return this.statement.getMoreResults();
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return this.statement.getGeneratedKeys();
    }

    private String playbackInvocation(String sql) throws SQLException {
        try {
            // 1. replace sql
            DDRDataSource.ReplacedResult replacedResult = replaceSql(sql, this.jdbcParameterForFirstAddBatch);
            // 2. check if crossing datasource
            if (isCrossDataSource(replacedResult.getSchemas())) {
                throw new DDRException("Sql '" + sql + "' query cross datasource");
            }
            if (statement == null) {
                // 3. init statement if not
                if (statement == null) {
                    DataSourceParam param = new DataSourceParam();
                    param.setReadOnly(readOnly);
                    param.setScNames(replacedResult.getSchemas());
                    statement = getStatement(param, replacedResult.getSql());
                }
            }

            // 3.1 回放set方法调用
            for (ExecutionContext context : executionContexts) {
                if (context.getStatementBatchSql() != null) {
                    statement.addBatch(context.getStatementBatchSql());
                } else {
                    for (InvokeRecord invokeRecord : context.getInvokeRecords()) {
                        statement.getClass().getMethod(invokeRecord.getMethodName(), invokeRecord.getParamTypes()).invoke(statement,
                                                                                                                          invokeRecord.getParams());
                    }
                    statement.addBatch();
                }
            }
            executionContexts.clear();
            return replacedResult.getSql();
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        sql = this.playbackInvocation(sql);
        return statement.executeUpdate(sql, autoGeneratedKeys);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        sql = this.playbackInvocation(sql);
        return this.statement.executeUpdate(sql, columnIndexes);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        sql = this.playbackInvocation(sql);
        return this.statement.executeUpdate(sql, columnNames);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        sql = this.playbackInvocation(sql);
        return this.statement.execute(sql, autoGeneratedKeys);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        sql = this.playbackInvocation(sql);
        return this.statement.execute(sql, columnIndexes);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        sql = this.playbackInvocation(sql);
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

    // /////////////////
    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setObject", new Object[] {
                                                                     parameterIndex, x }, new Class[] { int.class,
                                                                     Object.class, int.class }));
        } else {
            statement.setObject(parameterIndex, x, targetSqlType);
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setObject", new Object[] {
                                                                     parameterIndex, x }, new Class[] { int.class,
                                                                     Object.class }));
        } else {
            statement.setObject(parameterIndex, x);
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setCharacterStream", new Object[] {
                                                                     parameterIndex, reader, length }, new Class[] {
                                                                     int.class, Reader.class, int.class }));
        } else {
            statement.setCharacterStream(parameterIndex, reader, length);
        }
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setRef", new Object[] { parameterIndex,
                                                                     x }, new Class[] { int.class, Ref.class }));
        } else {
            statement.setRef(parameterIndex, x);
        }
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setBlob", new Object[] { parameterIndex,
                                                                     x }, new Class[] { int.class, Blob.class }));
        } else {
            statement.setBlob(parameterIndex, x);
        }
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setClob", new Object[] { parameterIndex,
                                                                     x }, new Class[] { int.class, Clob.class }));
        } else {
            statement.setClob(parameterIndex, x);
        }
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setArray", new Object[] {
                                                                     parameterIndex, x }, new Class[] { int.class,
                                                                     Array.class }));
        } else {
            statement.setArray(parameterIndex, x);
        }
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setDate", new Object[] { parameterIndex,
                                                                     x }, new Class[] { int.class, Date.class,
                                                                     Calendar.class }));
        } else {
            statement.setDate(parameterIndex, x, cal);
        }
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setTime", new Object[] { parameterIndex,
                                                                     x }, new Class[] { int.class, Time.class,
                                                                     Calendar.class }));

        } else {
            statement.setTime(parameterIndex, x, cal);
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setTimestamp", new Object[] {
                                                                     parameterIndex, x }, new Class[] { int.class,
                                                                     Timestamp.class, Calendar.class }));
        } else {
            statement.setTimestamp(parameterIndex, x, cal);
        }
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setJdbcParameter(parameterIndex, null);
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setNull", new Object[] { parameterIndex,
                                                                     sqlType, typeName }, new Class[] { int.class,
                                                                     int.class, String.class }));
        } else {
            statement.setNull(parameterIndex, sqlType, typeName);
        }
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setURL", new Object[] { parameterIndex,
                                                                     x }, new Class[] { int.class, URL.class }));
        } else {
            statement.setURL(parameterIndex, x);
        }
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setRowId", new Object[] {
                                                                     parameterIndex, x }, new Class[] { int.class,
                                                                     RowId.class }));
        } else {
            statement.setRowId(parameterIndex, x);
        }
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        setJdbcParameter(parameterIndex, value);
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setNString", new Object[] {
                                                                     parameterIndex, value }, new Class[] { int.class,
                                                                     String.class }));
        } else {
            statement.setNString(parameterIndex, value);
        }
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setNCharacterStream", new Object[] {
                                                                     parameterIndex, value, length }, new Class[] {
                                                                     int.class, Reader.class }));
        } else {
            statement.setNCharacterStream(parameterIndex, value, length);
        }
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        setJdbcParameter(parameterIndex, value);
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setNClob", new Object[] {
                                                                     parameterIndex, value }, new Class[] { int.class,
                                                                     NClob.class }));
        } else {
            statement.setNClob(parameterIndex, value);
        }
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        setJdbcParameter(parameterIndex, length);
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setClob", new Object[] { parameterIndex,
                                                                     reader, length }, new Class[] { int.class,
                                                                     Reader.class, long.class }));
        } else {
            statement.setClob(parameterIndex, reader, length);
        }
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setBlob", new Object[] { parameterIndex,
                                                                     inputStream, length }, new Class[] { int.class,
                                                                     InputStream.class, long.class }));
        } else {
            statement.setBlob(parameterIndex, inputStream, length);
        }
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setNClob", new Object[] {
                                                                     parameterIndex, reader, length }, new Class[] {
                                                                     int.class, Reader.class, long.class }));
        } else {
            statement.setNClob(parameterIndex, reader, length);
        }
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setSQLXML", new Object[] {
                                                                     parameterIndex, xmlObject }, new Class[] {
                                                                     int.class, SQLXML.class }));
        } else {
            statement.setSQLXML(parameterIndex, xmlObject);
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setObject", new Object[] {
                                                                     parameterIndex, x, targetSqlType, scaleOrLength },
                                                                              new Class[] { int.class, Object.class,
                                                                                      int.class, int.class }));
        } else {
            statement.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setAsciiStream", new Object[] {
                                                                     parameterIndex, x, length }, new Class[] {
                                                                     int.class, InputStream.class, long.class }));
        } else {
            statement.setAsciiStream(parameterIndex, x, length);
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setBinaryStream", new Object[] {
                                                                     parameterIndex, x, length }, new Class[] {
                                                                     int.class, InputStream.class, long.class }));
        } else {
            statement.setBinaryStream(parameterIndex, x, length);
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setCharacterStream", new Object[] {
                                                                     parameterIndex, reader, length }, new Class[] {
                                                                     int.class, Reader.class, long.class }));
        } else {
            statement.setCharacterStream(parameterIndex, reader, length);
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setAsciiStream", new Object[] {
                                                                     parameterIndex, x }, new Class[] { int.class,
                                                                     InputStream.class }));
        } else {
            statement.setAsciiStream(parameterIndex, x);
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setBinaryStream", new Object[] {
                                                                     parameterIndex, x }, new Class[] { int.class,
                                                                     InputStream.class }));
        } else {
            statement.setBinaryStream(parameterIndex, x);
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setCharacterStream", new Object[] {
                                                                     parameterIndex, reader }, new Class[] { int.class,
                                                                     Reader.class }));
        } else {
            statement.setCharacterStream(parameterIndex, reader);
        }
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setNCharacterStream", new Object[] {
                                                                     parameterIndex, value }, new Class[] { int.class,
                                                                     Reader.class }));
        } else {
            statement.setNCharacterStream(parameterIndex, value);
        }
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setClob", new Object[] { parameterIndex,
                                                                     reader }, new Class[] { int.class, Reader.class }));
        } else {
            statement.setClob(parameterIndex, reader);
        }
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setBlob", new Object[] { parameterIndex,
                                                                     inputStream }, new Class[] { int.class,
                                                                     InputStream.class }));
        } else {
            statement.setBlob(parameterIndex, inputStream);
        }
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setNClob", new Object[] {
                                                                     parameterIndex, reader }, new Class[] { int.class,
                                                                     Reader.class }));
        } else {
            statement.setNClob(parameterIndex, reader);
        }
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setQueryTimeout",
                                                                              new Object[] { seconds },
                                                                              new Class[] { int.class }));
        } else {
            statement.setQueryTimeout(seconds);
        }
    }

    private void setJdbcParameter(int index, Object val) {
        if (statement == null) {
            this.jdbcParameterForFirstAddBatch.put(index, val);
        }
        this.jdbcParameter.put(index, val);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        setJdbcParameter(parameterIndex, sqlType);
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setNull", new Object[] { parameterIndex,
                                                                     sqlType }, new Class[] { int.class, int.class }));
        } else {
            statement.setNull(parameterIndex, sqlType);
        }
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setBoolean", new Object[] {
                                                                     parameterIndex, x }, new Class[] { int.class,
                                                                     boolean.class }));
        } else {
            statement.setBoolean(parameterIndex, x);
        }
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setByte", new Object[] { parameterIndex,
                                                                     x }, new Class[] { int.class, byte.class }));
        } else {
            statement.setByte(parameterIndex, x);
        }
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setShort", new Object[] {
                                                                     parameterIndex, x }, new Class[] { int.class,
                                                                     short.class }));
        } else {
            statement.setShort(parameterIndex, x);
        }
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setInt", new Object[] { parameterIndex,
                                                                     x }, new Class[] { int.class, int.class }));
        } else {
            statement.setInt(parameterIndex, x);
        }
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setLong", new Object[] { parameterIndex,
                                                                     x }, new Class[] { int.class, long.class }));
        } else {
            statement.setLong(parameterIndex, x);
        }
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (statement == null) {

            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setFloat", new Object[] {
                                                                     parameterIndex, x }, new Class[] { int.class,
                                                                     float.class }));
        } else {
            statement.setFloat(parameterIndex, x);
        }

    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setDouble", new Object[] {
                                                                     parameterIndex, x }, new Class[] { int.class,
                                                                     double.class }));
        } else {
            statement.setDouble(parameterIndex, x);
        }
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setBigDecimal", new Object[] {
                                                                     parameterIndex, x }, new Class[] { int.class,
                                                                     BigDecimal.class }));
        } else {
            statement.setBigDecimal(parameterIndex, x);
        }

    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        setJdbcParameter(parameterIndex, x);
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setString", new Object[] {
                                                                     parameterIndex, x }, new Class[] { int.class,
                                                                     String.class }));
        } else {
            statement.setString(parameterIndex, x);
        }
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        setJdbcParameter(parameterIndex, x);
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setBytes", new Object[] {
                                                                     parameterIndex, x }, new Class[] { int.class,
                                                                     byte[].class }));
        } else {
            statement.setBytes(parameterIndex, x);
        }
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        setJdbcParameter(parameterIndex, x);
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setDate", new Object[] { parameterIndex,
                                                                     x }, new Class[] { int.class, Date.class }));
        } else {
            statement.setDate(parameterIndex, x);
        }
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        setJdbcParameter(parameterIndex, x);
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setTime", new Object[] { parameterIndex,
                                                                     x }, new Class[] { int.class, Time.class }));
        } else {
            statement.setTime(parameterIndex, x);
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        setJdbcParameter(parameterIndex, x);
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setTimestamp", new Object[] {
                                                                     parameterIndex, x }, new Class[] { int.class,
                                                                     Timestamp.class }));
        } else {
            statement.setTimestamp(parameterIndex, x);
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setAsciiStream", new Object[] {
                                                                     parameterIndex, x }, new Class[] { int.class,
                                                                     InputStream.class, int.class }));
        } else {
            statement.setAsciiStream(parameterIndex, x, length);
        }

    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setUnicodeStream", new Object[] {
                                                                     parameterIndex, x }, new Class[] { int.class,
                                                                     InputStream.class, int.class }));
        } else {
            statement.setUnicodeStream(parameterIndex, x, length);
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        if (statement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setBinaryStream", new Object[] {
                                                                     parameterIndex, x }, new Class[] { int.class,
                                                                     InputStream.class, int.class }));
        } else {
            statement.setBinaryStream(parameterIndex, x, length);
        }
    }
}
