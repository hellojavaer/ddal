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
public abstract class PreparedStatementWrapper extends StatementWrapper implements DDRPreparedStatement {

    private PreparedStatement    preparedStatement;
    private String               sql;
    private Map<Integer, Object> jdbcParameterForFirstAddBatch = new HashMap<Integer, Object>();
    private Map<Integer, Object> jdbcParameter                 = new HashMap<Integer, Object>();

    public PreparedStatementWrapper(String sql, boolean readOnly) {
        super(readOnly);
        this.sql = sql;
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

    // ////pre
    @Override
    public ResultSet executeQuery() throws SQLException {
        initPreparedStatement();
        return this.preparedStatement.executeQuery();
    }

    @Override
    public int executeUpdate() throws SQLException {
        initPreparedStatement();
        return this.preparedStatement.executeUpdate();
    }

    @Override
    public boolean execute() throws SQLException {
        initPreparedStatement();
        return preparedStatement.execute();
    }

    // PreparedStatement Override
    @Override
    public int[] executeBatch() throws SQLException {
        initPreparedStatement();
        return preparedStatement.executeBatch();
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
    public void clearBatch() throws SQLException {
        this.executionContexts.clear();
        this.pushNewExecutionContext();
        super.clearBatch();
    }

    @Override
    public void addBatch() throws SQLException {
        this.pushNewExecutionContext();
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        this.pushNewExecutionContext(sql);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        if (preparedStatement != null) {
            return preparedStatement.getParameterMetaData();
        } else {
            throw new DDRException("Can't invoke 'getParameterMetaData()' before preparedStatement is initialized");
        }
    }

    @Override
    public int getUpdateCount() throws SQLException {
        if (preparedStatement != null) {
            return preparedStatement.getUpdateCount();
        } else {
            throw new DDRException("Can't invoke 'getUpdateCount()' before preparedStatement is initialized");
        }
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        if (preparedStatement != null) {
            return this.preparedStatement.getResultSetConcurrency();
        } else {
            throw new DDRException("Can't invoke 'getResultSetConcurrency()' before preparedStatement is initialized");
        }
    }

    private void initPreparedStatement() throws SQLException {
        // 1. replace sql
        DDRDataSource.ReplacedResult replacedResult = replaceSql(sql, this.jdbcParameterForFirstAddBatch);
        // 2. check if crossing datasource
        if (isCrossDataSource(replacedResult.getSchemas())) {
            throw new DDRException("Sql '" + sql + "' query cross datasource");
        }
        // 3. init preparedStatement if not
        if (preparedStatement == null) {
            DataSourceParam param = new DataSourceParam();
            param.setReadOnly(readOnly);
            param.setScNames(replacedResult.getSchemas());
            initStatement(param, replacedResult.getSql());
            super.playbackInvocation(statement);
        }
        // 4. 动作回放
        playbackInvocation(preparedStatement);
    }

    @Override
    protected void initStatement(DataSourceParam param, String sql) throws SQLException {
        preparedStatement = (PreparedStatement) getStatement(param, sql);
        statement = preparedStatement;
    }

    protected void playbackInvocation(PreparedStatement preparedStatement) throws SQLException {
        try {
            for (ExecutionContext context : executionContexts) {
                if (context.getStatementBatchSql() != null) {
                    preparedStatement.addBatch(context.getStatementBatchSql());
                } else {
                    for (InvokeRecord invokeRecord : context.getInvokeRecords()) {
                        preparedStatement.getClass().getMethod(invokeRecord.getMethodName(),
                                                               invokeRecord.getParamTypes()).invoke(preparedStatement,
                                                                                                    invokeRecord.getParams());
                    }
                    preparedStatement.addBatch();
                }
            }
            executionContexts.clear();
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    // /////////////////
    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setObject", new Object[] {
                                                                     parameterIndex, x }, new Class[] { int.class,
                                                                     Object.class, int.class }));
        } else {
            preparedStatement.setObject(parameterIndex, x, targetSqlType);
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setObject", new Object[] {
                                                                     parameterIndex, x }, new Class[] { int.class,
                                                                     Object.class }));
        } else {
            preparedStatement.setObject(parameterIndex, x);
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setCharacterStream", new Object[] {
                                                                     parameterIndex, reader, length }, new Class[] {
                                                                     int.class, Reader.class, int.class }));
        } else {
            preparedStatement.setCharacterStream(parameterIndex, reader, length);
        }
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setRef", new Object[] { parameterIndex,
                                                                     x }, new Class[] { int.class, Ref.class }));
        } else {
            preparedStatement.setRef(parameterIndex, x);
        }
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setBlob", new Object[] { parameterIndex,
                                                                     x }, new Class[] { int.class, Blob.class }));
        } else {
            preparedStatement.setBlob(parameterIndex, x);
        }
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setClob", new Object[] { parameterIndex,
                                                                     x }, new Class[] { int.class, Clob.class }));
        } else {
            preparedStatement.setClob(parameterIndex, x);
        }
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setArray", new Object[] {
                                                                     parameterIndex, x }, new Class[] { int.class,
                                                                     Array.class }));
        } else {
            preparedStatement.setArray(parameterIndex, x);
        }
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setDate", new Object[] { parameterIndex,
                                                                     x }, new Class[] { int.class, Date.class,
                                                                     Calendar.class }));
        } else {
            preparedStatement.setDate(parameterIndex, x, cal);
        }
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setTime", new Object[] { parameterIndex,
                                                                     x }, new Class[] { int.class, Time.class,
                                                                     Calendar.class }));

        } else {
            preparedStatement.setTime(parameterIndex, x, cal);
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setTimestamp", new Object[] {
                                                                     parameterIndex, x }, new Class[] { int.class,
                                                                     Timestamp.class, Calendar.class }));
        } else {
            preparedStatement.setTimestamp(parameterIndex, x, cal);
        }
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setJdbcParameter(parameterIndex, null);
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setNull", new Object[] { parameterIndex,
                                                                     sqlType, typeName }, new Class[] { int.class,
                                                                     int.class, String.class }));
        } else {
            preparedStatement.setNull(parameterIndex, sqlType, typeName);
        }
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setURL", new Object[] { parameterIndex,
                                                                     x }, new Class[] { int.class, URL.class }));
        } else {
            preparedStatement.setURL(parameterIndex, x);
        }
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setRowId", new Object[] {
                                                                     parameterIndex, x }, new Class[] { int.class,
                                                                     RowId.class }));
        } else {
            preparedStatement.setRowId(parameterIndex, x);
        }
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        setJdbcParameter(parameterIndex, value);
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setNString", new Object[] {
                                                                     parameterIndex, value }, new Class[] { int.class,
                                                                     String.class }));
        } else {
            preparedStatement.setNString(parameterIndex, value);
        }
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setNCharacterStream", new Object[] {
                                                                     parameterIndex, value, length }, new Class[] {
                                                                     int.class, Reader.class }));
        } else {
            preparedStatement.setNCharacterStream(parameterIndex, value, length);
        }
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        setJdbcParameter(parameterIndex, value);
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setNClob", new Object[] {
                                                                     parameterIndex, value }, new Class[] { int.class,
                                                                     NClob.class }));
        } else {
            preparedStatement.setNClob(parameterIndex, value);
        }
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        setJdbcParameter(parameterIndex, length);
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setClob", new Object[] { parameterIndex,
                                                                     reader, length }, new Class[] { int.class,
                                                                     Reader.class, long.class }));
        } else {
            preparedStatement.setClob(parameterIndex, reader, length);
        }
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setBlob", new Object[] { parameterIndex,
                                                                     inputStream, length }, new Class[] { int.class,
                                                                     InputStream.class, long.class }));
        } else {
            preparedStatement.setBlob(parameterIndex, inputStream, length);
        }
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setNClob", new Object[] {
                                                                     parameterIndex, reader, length }, new Class[] {
                                                                     int.class, Reader.class, long.class }));
        } else {
            preparedStatement.setNClob(parameterIndex, reader, length);
        }
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setSQLXML", new Object[] {
                                                                     parameterIndex, xmlObject }, new Class[] {
                                                                     int.class, SQLXML.class }));
        } else {
            preparedStatement.setSQLXML(parameterIndex, xmlObject);
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setObject", new Object[] {
                                                                     parameterIndex, x, targetSqlType, scaleOrLength },
                                                                              new Class[] { int.class, Object.class,
                                                                                      int.class, int.class }));
        } else {
            preparedStatement.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setAsciiStream", new Object[] {
                                                                     parameterIndex, x, length }, new Class[] {
                                                                     int.class, InputStream.class, long.class }));
        } else {
            preparedStatement.setAsciiStream(parameterIndex, x, length);
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setBinaryStream", new Object[] {
                                                                     parameterIndex, x, length }, new Class[] {
                                                                     int.class, InputStream.class, long.class }));
        } else {
            preparedStatement.setBinaryStream(parameterIndex, x, length);
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setCharacterStream", new Object[] {
                                                                     parameterIndex, reader, length }, new Class[] {
                                                                     int.class, Reader.class, long.class }));
        } else {
            preparedStatement.setCharacterStream(parameterIndex, reader, length);
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setAsciiStream", new Object[] {
                                                                     parameterIndex, x }, new Class[] { int.class,
                                                                     InputStream.class }));
        } else {
            preparedStatement.setAsciiStream(parameterIndex, x);
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setBinaryStream", new Object[] {
                                                                     parameterIndex, x }, new Class[] { int.class,
                                                                     InputStream.class }));
        } else {
            preparedStatement.setBinaryStream(parameterIndex, x);
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setCharacterStream", new Object[] {
                                                                     parameterIndex, reader }, new Class[] { int.class,
                                                                     Reader.class }));
        } else {
            preparedStatement.setCharacterStream(parameterIndex, reader);
        }
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setNCharacterStream", new Object[] {
                                                                     parameterIndex, value }, new Class[] { int.class,
                                                                     Reader.class }));
        } else {
            preparedStatement.setNCharacterStream(parameterIndex, value);
        }
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setClob", new Object[] { parameterIndex,
                                                                     reader }, new Class[] { int.class, Reader.class }));
        } else {
            preparedStatement.setClob(parameterIndex, reader);
        }
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setBlob", new Object[] { parameterIndex,
                                                                     inputStream }, new Class[] { int.class,
                                                                     InputStream.class }));
        } else {
            preparedStatement.setBlob(parameterIndex, inputStream);
        }
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setNClob", new Object[] {
                                                                     parameterIndex, reader }, new Class[] { int.class,
                                                                     Reader.class }));
        } else {
            preparedStatement.setNClob(parameterIndex, reader);
        }
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setQueryTimeout",
                                                                              new Object[] { seconds },
                                                                              new Class[] { int.class }));
        } else {
            preparedStatement.setQueryTimeout(seconds);
        }
    }

    private void setJdbcParameter(int index, Object val) {
        if (preparedStatement == null) {
            this.jdbcParameterForFirstAddBatch.put(index, val);
        }
        this.jdbcParameter.put(index, val);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        setJdbcParameter(parameterIndex, sqlType);
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setNull", new Object[] { parameterIndex,
                                                                     sqlType }, new Class[] { int.class, int.class }));
        } else {
            preparedStatement.setNull(parameterIndex, sqlType);
        }
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setBoolean", new Object[] {
                                                                     parameterIndex, x }, new Class[] { int.class,
                                                                     boolean.class }));
        } else {
            preparedStatement.setBoolean(parameterIndex, x);
        }
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setByte", new Object[] { parameterIndex,
                                                                     x }, new Class[] { int.class, byte.class }));
        } else {
            preparedStatement.setByte(parameterIndex, x);
        }
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setShort", new Object[] {
                                                                     parameterIndex, x }, new Class[] { int.class,
                                                                     short.class }));
        } else {
            preparedStatement.setShort(parameterIndex, x);
        }
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setInt", new Object[] { parameterIndex,
                                                                     x }, new Class[] { int.class, int.class }));
        } else {
            preparedStatement.setInt(parameterIndex, x);
        }
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setLong", new Object[] { parameterIndex,
                                                                     x }, new Class[] { int.class, long.class }));
        } else {
            preparedStatement.setLong(parameterIndex, x);
        }
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (preparedStatement == null) {

            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setFloat", new Object[] {
                                                                     parameterIndex, x }, new Class[] { int.class,
                                                                     float.class }));
        } else {
            preparedStatement.setFloat(parameterIndex, x);
        }

    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setDouble", new Object[] {
                                                                     parameterIndex, x }, new Class[] { int.class,
                                                                     double.class }));
        } else {
            preparedStatement.setDouble(parameterIndex, x);
        }
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setBigDecimal", new Object[] {
                                                                     parameterIndex, x }, new Class[] { int.class,
                                                                     BigDecimal.class }));
        } else {
            preparedStatement.setBigDecimal(parameterIndex, x);
        }

    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        setJdbcParameter(parameterIndex, x);
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setString", new Object[] {
                                                                     parameterIndex, x }, new Class[] { int.class,
                                                                     String.class }));
        } else {
            preparedStatement.setString(parameterIndex, x);
        }
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        setJdbcParameter(parameterIndex, x);
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setBytes", new Object[] {
                                                                     parameterIndex, x }, new Class[] { int.class,
                                                                     byte[].class }));
        } else {
            preparedStatement.setBytes(parameterIndex, x);
        }
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        setJdbcParameter(parameterIndex, x);
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setDate", new Object[] { parameterIndex,
                                                                     x }, new Class[] { int.class, Date.class }));
        } else {
            preparedStatement.setDate(parameterIndex, x);
        }
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        setJdbcParameter(parameterIndex, x);
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setTime", new Object[] { parameterIndex,
                                                                     x }, new Class[] { int.class, Time.class }));
        } else {
            preparedStatement.setTime(parameterIndex, x);
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        setJdbcParameter(parameterIndex, x);
        setJdbcParameter(parameterIndex, x);
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setTimestamp", new Object[] {
                                                                     parameterIndex, x }, new Class[] { int.class,
                                                                     Timestamp.class }));
        } else {
            preparedStatement.setTimestamp(parameterIndex, x);
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setAsciiStream", new Object[] {
                                                                     parameterIndex, x }, new Class[] { int.class,
                                                                     InputStream.class, int.class }));
        } else {
            preparedStatement.setAsciiStream(parameterIndex, x, length);
        }

    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setUnicodeStream", new Object[] {
                                                                     parameterIndex, x }, new Class[] { int.class,
                                                                     InputStream.class, int.class }));
        } else {
            preparedStatement.setUnicodeStream(parameterIndex, x, length);
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        if (preparedStatement == null) {
            this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setBinaryStream", new Object[] {
                                                                     parameterIndex, x }, new Class[] { int.class,
                                                                     InputStream.class, int.class }));
        } else {
            preparedStatement.setBinaryStream(parameterIndex, x, length);
        }
    }

    // 特殊处理
    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        if (preparedStatement != null) {
            return preparedStatement.getMetaData();
        } else {
            return null;
        }
    }
}
