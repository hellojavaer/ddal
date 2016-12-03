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
abstract class StatementWrapper implements PreparedStatement {

    private Statement  statement;

    private Connection connection;
    private String     sql;
    private String     createStatementMethodName;
    private Object[]   createStatementMethodParams;
    private Class[]    createStatementMethodParamTypes;

    public StatementWrapper(Connection connection, String sql, String createStatementMethodName,
                            Object[] createStatementMethodParams, Class[] createStatementMethodParamTypes) {
        this.connection = connection;
        this.sql = sql;
        this.createStatementMethodName = createStatementMethodName;
        this.createStatementMethodParams = createStatementMethodParams;
        this.createStatementMethodParamTypes = createStatementMethodParamTypes;
        this.pushNewParamConext();
    }

    private List<ParamContext> paramContexts = new ArrayList<ParamContext>();

    private ParamContext getCurParamContext() {
        return paramContexts.get(paramContexts.size() - 1);
    }

    private void pushNewParamConext() {
        paramContexts.add(new ParamContext());
    }

    private class ParamContext {

        private Map<Integer, Object> jdbcParams    = new LinkedHashMap<Integer, Object>();
        private List<InvokeRecord>   invokeRecords = new ArrayList<InvokeRecord>();

        public Map<Integer, Object> getJdbcParams() {
            return jdbcParams;
        }

        public void setJdbcParams(Map<Integer, Object> jdbcParams) {
            this.jdbcParams = jdbcParams;
        }

        public List<InvokeRecord> getInvokeRecords() {
            return invokeRecords;
        }

        public void setInvokeRecords(List<InvokeRecord> invokeRecords) {
            this.invokeRecords = invokeRecords;
        }
    }

    private class InvokeRecord {

        private String   addBatchSql;

        private String   methodName;
        private Object[] params;
        private Class[]  paramTypes;

        public InvokeRecord(String addBatchSql) {
            this.addBatchSql = addBatchSql;
        }

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

        public String getAddBatchSql() {
            return addBatchSql;
        }

        public void setAddBatchSql(String addBatchSql) {
            this.addBatchSql = addBatchSql;
        }

        public Class[] getParamTypes() {
            return paramTypes;
        }

        public void setParamTypes(Class[] paramTypes) {
            this.paramTypes = paramTypes;
        }
    }

    public abstract String replaceSql(String sql, Map<Integer, Object> jdbcParams);

    @Override
    public ResultSet executeQuery() throws SQLException {

        return null;
    }

    @Override
    public int executeUpdate() throws SQLException {
        return 0;
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        this.getCurParamContext().getJdbcParams().put(parameterIndex, sqlType);
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setNull", new Object[] { parameterIndex,
                                                                 sqlType }, new Class[] { int.class, int.class }));
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        this.getCurParamContext().getJdbcParams().put(parameterIndex, x);
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setBoolean", new Object[] { parameterIndex,
                                                                 x }, new Class[] { int.class, boolean.class }));
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        this.getCurParamContext().getJdbcParams().put(parameterIndex, x);
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setByte",
                                                                          new Object[] { parameterIndex, x },
                                                                          new Class[] { int.class, byte.class }));
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        this.getCurParamContext().getJdbcParams().put(parameterIndex, x);
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setShort",
                                                                          new Object[] { parameterIndex, x },
                                                                          new Class[] { int.class, short.class }));
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        this.getCurParamContext().getJdbcParams().put(parameterIndex, x);
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setInt", new Object[] { parameterIndex, x },
                                                                          new Class[] { int.class, int.class }));
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        this.getCurParamContext().getJdbcParams().put(parameterIndex, x);
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setLong",
                                                                          new Object[] { parameterIndex, x },
                                                                          new Class[] { int.class, long.class }));
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        this.getCurParamContext().getJdbcParams().put(parameterIndex, x);
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setFloat",
                                                                          new Object[] { parameterIndex, x },
                                                                          new Class[] { int.class, float.class }));
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        this.getCurParamContext().getJdbcParams().put(parameterIndex, x);
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setDouble",
                                                                          new Object[] { parameterIndex, x },
                                                                          new Class[] { int.class, double.class }));
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        this.getCurParamContext().getJdbcParams().put(parameterIndex, x);
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setDouble",
                                                                          new Object[] { parameterIndex, x },
                                                                          new Class[] { int.class, BigDecimal.class }));
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        this.getCurParamContext().getJdbcParams().put(parameterIndex, x);
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setString",
                                                                          new Object[] { parameterIndex, x },
                                                                          new Class[] { int.class, String.class }));
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        this.getCurParamContext().getJdbcParams().put(parameterIndex, x);
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setBytes",
                                                                          new Object[] { parameterIndex, x },
                                                                          new Class[] { int.class, byte.class }));
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        this.getCurParamContext().getJdbcParams().put(parameterIndex, x);
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setDate",
                                                                          new Object[] { parameterIndex, x },
                                                                          new Class[] { int.class, Date.class }));
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        this.getCurParamContext().getJdbcParams().put(parameterIndex, x);
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setTime",
                                                                          new Object[] { parameterIndex, x },
                                                                          new Class[] { int.class, Time.class }));
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        this.getCurParamContext().getJdbcParams().put(parameterIndex, x);
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setTimestamp", new Object[] {
                                                                 parameterIndex, x }, new Class[] { int.class,
                                                                 Timestamp.class }));
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        this.getCurParamContext().getJdbcParams().put(parameterIndex, x);
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setAsciiStream", new Object[] {
                                                                 parameterIndex, x }, new Class[] { int.class,
                                                                 InputStream.class, int.class }));
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        this.getCurParamContext().getJdbcParams().put(parameterIndex, x);
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setUnicodeStream", new Object[] {
                                                                 parameterIndex, x }, new Class[] { int.class,
                                                                 InputStream.class, int.class }));
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        this.getCurParamContext().getJdbcParams().put(parameterIndex, x);
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setBinaryStream", new Object[] {
                                                                 parameterIndex, x }, new Class[] { int.class,
                                                                 InputStream.class, int.class }));
    }

    @Override
    public void clearParameters() throws SQLException {
        this.getCurParamContext().getJdbcParams().clear();
        this.getCurParamContext().getInvokeRecords();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        this.getCurParamContext().getJdbcParams().put(parameterIndex, x);
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setObject",
                                                                          new Object[] { parameterIndex, x },
                                                                          new Class[] { int.class, Object.class,
                                                                                  int.class }));
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        this.getCurParamContext().getJdbcParams().put(parameterIndex, x);
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setObject",
                                                                          new Object[] { parameterIndex, x },
                                                                          new Class[] { int.class, Object.class }));
    }

    @Override
    public boolean execute() throws SQLException {
        this.initStatement(this.sql);
        return ((PreparedStatement) this.statement).execute();
    }

    @Override
    public void addBatch() throws SQLException {
        this.pushNewParamConext();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setCharacterStream", new Object[] {
                                                                 parameterIndex, reader, length }, new Class[] {
                                                                 int.class, Reader.class, int.class }));
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        this.getCurParamContext().getJdbcParams().put(parameterIndex, x);
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setRef", new Object[] { parameterIndex, x },
                                                                          new Class[] { int.class, Ref.class }));
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        this.getCurParamContext().getJdbcParams().put(parameterIndex, x);
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setBlob",
                                                                          new Object[] { parameterIndex, x },
                                                                          new Class[] { int.class, Blob.class }));
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        this.getCurParamContext().getJdbcParams().put(parameterIndex, x);
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setClob",
                                                                          new Object[] { parameterIndex, x },
                                                                          new Class[] { int.class, Clob.class }));
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        this.getCurParamContext().getJdbcParams().put(parameterIndex, x);
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setArray",
                                                                          new Object[] { parameterIndex, x },
                                                                          new Class[] { int.class, Array.class }));
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        this.getCurParamContext().getJdbcParams().put(parameterIndex, x);
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setDate",
                                                                          new Object[] { parameterIndex, x },
                                                                          new Class[] { int.class, Date.class,
                                                                                  Calendar.class }));
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        this.getCurParamContext().getJdbcParams().put(parameterIndex, x);
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setTime",
                                                                          new Object[] { parameterIndex, x },
                                                                          new Class[] { int.class, Time.class,
                                                                                  Calendar.class }));
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        this.getCurParamContext().getJdbcParams().put(parameterIndex, x);
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setTimestamp", new Object[] {
                                                                 parameterIndex, x }, new Class[] { int.class,
                                                                 Timestamp.class, Calendar.class }));
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        this.getCurParamContext().getJdbcParams().put(parameterIndex, null);
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setNull", new Object[] { parameterIndex,
                                                                 sqlType, typeName }, new Class[] { int.class,
                                                                 int.class, String.class }));
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        this.getCurParamContext().getJdbcParams().put(parameterIndex, x);
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setURL", new Object[] { parameterIndex, x },
                                                                          new Class[] { int.class, URL.class }));
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new UnsupportedOperationException("getParameterMetaData");
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        this.getCurParamContext().getJdbcParams().put(parameterIndex, x);
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setRowId",
                                                                          new Object[] { parameterIndex, x },
                                                                          new Class[] { int.class, RowId.class }));
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        this.getCurParamContext().getJdbcParams().put(parameterIndex, value);
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setNString", new Object[] { parameterIndex,
                                                                 value }, new Class[] { int.class, String.class }));
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setNCharacterStream", new Object[] {
                                                                 parameterIndex, value, length }, new Class[] {
                                                                 int.class, Reader.class }));
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        this.getCurParamContext().getJdbcParams().put(parameterIndex, value);
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setNClob", new Object[] { parameterIndex,
                                                                 value }, new Class[] { int.class, NClob.class }));
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        this.getCurParamContext().getJdbcParams().put(parameterIndex, length);
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setClob", new Object[] { parameterIndex,
                                                                 reader, length }, new Class[] { int.class,
                                                                 Reader.class, long.class }));
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setBlob", new Object[] { parameterIndex,
                                                                 inputStream, length }, new Class[] { int.class,
                                                                 InputStream.class, long.class }));
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setNClob", new Object[] { parameterIndex,
                                                                 reader, length }, new Class[] { int.class,
                                                                 Reader.class, long.class }));
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setSQLXML", new Object[] { parameterIndex,
                                                                 xmlObject }, new Class[] { int.class, SQLXML.class }));
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        this.getCurParamContext().getJdbcParams().put(parameterIndex, x);
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setObject", new Object[] { parameterIndex,
                                                                 x, targetSqlType, scaleOrLength }, new Class[] {
                                                                 int.class, Object.class, int.class, int.class }));
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        this.getCurParamContext().getJdbcParams().put(parameterIndex, x);
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setAsciiStream", new Object[] {
                                                                 parameterIndex, x, length }, new Class[] { int.class,
                                                                 InputStream.class, long.class }));
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setBinaryStream", new Object[] {
                                                                 parameterIndex, x, length }, new Class[] { int.class,
                                                                 InputStream.class, long.class }));
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setCharacterStream", new Object[] {
                                                                 parameterIndex, reader, length }, new Class[] {
                                                                 int.class, Reader.class, long.class }));
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setAsciiStream", new Object[] {
                                                                 parameterIndex, x }, new Class[] { int.class,
                                                                 InputStream.class }));
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setBinaryStream", new Object[] {
                                                                 parameterIndex, x }, new Class[] { int.class,
                                                                 InputStream.class }));
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setCharacterStream", new Object[] {
                                                                 parameterIndex, reader }, new Class[] { int.class,
                                                                 Reader.class }));
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setNCharacterStream", new Object[] {
                                                                 parameterIndex, value }, new Class[] { int.class,
                                                                 Reader.class }));
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setClob", new Object[] { parameterIndex,
                                                                 reader }, new Class[] { int.class, Reader.class }));
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setBlob", new Object[] { parameterIndex,
                                                                 inputStream }, new Class[] { int.class,
                                                                 InputStream.class }));
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setNClob", new Object[] { parameterIndex,
                                                                 reader }, new Class[] { int.class, Reader.class }));
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        sql = this.initStatement(sql);
        return this.statement.executeQuery(sql);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        sql = this.initStatement(sql);
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
    public void setQueryTimeout(int seconds) throws SQLException {
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord("setQueryTimeout", new Object[] { seconds },
                                                                          new Class[] { int.class }));
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
        sql = this.initStatement(sql);
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
        this.pushNewParamConext();
        this.getCurParamContext().getInvokeRecords().add(new InvokeRecord(sql));
    }

    @Override
    public void clearBatch() throws SQLException {
        this.paramContexts.clear();
        this.pushNewParamConext();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        this.initStatement(this.sql);
        return this.statement.executeBatch();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.connection;// //FIXME
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return this.statement.getMoreResults();
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return this.statement.getGeneratedKeys();
    }

    // =======播放执行动作
    private String initStatement(String sql) throws SQLException {
        try {
            // 1. 替换 sql
            String targetSql = replaceSql(sql, this.paramContexts.get(0).getJdbcParams());

            // 2. 创建 statement
            if("prepareStatement".equals(createStatementMethodName)){
                createStatementMethodParams[0] = targetSql;
            }
            Statement statement = (Statement) connection.getClass().getMethod(createStatementMethodName,
                                                                              createStatementMethodParamTypes).invoke(connection,
                                                                                                                      createStatementMethodParams);
            // 2.1 回放
            List<InvokeRecord> invokeRecords = this.paramContexts.get(0).getInvokeRecords();

            for (InvokeRecord invokeRecord : invokeRecords) {
                statement.getClass().getMethod(invokeRecord.getMethodName(), invokeRecord.getParamTypes()).invoke(statement,
                                                                                                                  invokeRecord.getParams());
            }

            // 3. 重新播放执行操作
            for (int i = 1; i < paramContexts.size(); i++) {
                ParamContext context = paramContexts.get(i);
                if (!context.getJdbcParams().isEmpty()) {
                    throw new UnsupportedOperationException("not support addBatch now");// TODO
                } else {
                    for (InvokeRecord record : context.getInvokeRecords()) {
                        String batchSql = record.getAddBatchSql();
                        batchSql = replaceSql(sql, null);
                        statement.addBatch(batchSql);
                    }
                }
            }
            this.statement = statement;
            return targetSql;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        sql = this.initStatement(sql);
        return statement.executeUpdate(sql, autoGeneratedKeys);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        sql = this.initStatement(sql);
        return this.statement.executeUpdate(sql, columnIndexes);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        sql = this.initStatement(sql);
        return this.statement.executeUpdate(sql, columnNames);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        sql = this.initStatement(sql);
        return this.statement.execute(sql, autoGeneratedKeys);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        sql = this.initStatement(sql);
        return this.statement.execute(sql, columnIndexes);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        sql = this.initStatement(sql);
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
}
