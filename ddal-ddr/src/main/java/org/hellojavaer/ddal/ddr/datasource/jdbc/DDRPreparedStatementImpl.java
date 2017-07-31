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
import org.hellojavaer.ddal.ddr.datasource.exception.UnsupportedPreparedStatementInvocationException;
import org.hellojavaer.ddal.ddr.datasource.manager.DataSourceParam;
import org.hellojavaer.ddal.ddr.sqlparse.SQLParsedResult;
import org.hellojavaer.ddal.ddr.utils.DDRJSONUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

/**
 *
 * 并发设置参数
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 20/11/2016.
 */
public abstract class DDRPreparedStatementImpl extends DDRStatementImpl implements DDRPreparedStatement {

    private Logger                    stdLogger               = LoggerFactory.getLogger("org.hellojavaer.ddr.sql");

    private String                    sql                     = null;
    protected PreparedStatement       preparedStatement       = null;
    private Map<Object, Object>       jdbcParameter           = new HashMap<Object, Object>();
    private List<JdbcParamInvocation> jdbcParamInvocationList = null;

    private SQLParsedResult           sqlParsedResult         = null;

    public DDRPreparedStatementImpl(String sql, boolean readOnly, Set<String> schemas) {
        super(readOnly, schemas);
        this.sql = sql;
    }

    private class JdbcParamInvocation {

        private JdbcParamSetMethod method;
        private int                index;
        private Object[]           params;

        public JdbcParamInvocation(JdbcParamSetMethod method, int index, Object[] params) {
            this.method = method;
            this.index = index;
            this.params = params;
        }

        public JdbcParamSetMethod getMethod() {
            return method;
        }

        public void setMethod(JdbcParamSetMethod method) {
            this.method = method;
        }

        public int getIndex() {
            return index;
        }

        public void setIndex(int index) {
            this.index = index;
        }

        public Object[] getParams() {
            return params;
        }

        public void setParams(Object[] params) {
            this.params = params;
        }

        public Class<?> getParamTypes() {
            return null;
        }

        public String getMethodName() {
            return null;
        }

    }

    // ////pre
    @Override
    public ResultSet executeQuery() throws SQLException {
        initPreparedStatementIfAbsent();
        return preparedStatement.executeQuery();
    }

    @Override
    public int executeUpdate() throws SQLException {
        initPreparedStatementIfAbsent();
        return preparedStatement.executeUpdate();
    }

    @Override
    public boolean execute() throws SQLException {
        initPreparedStatementIfAbsent();
        return preparedStatement.execute();
    }

    // PreparedStatement Override
    @Override
    public int[] executeBatch() throws SQLException {
        initPreparedStatementIfAbsent();
        return preparedStatement.executeBatch();
    }

    @Override
    public void clearParameters() throws SQLException {
        jdbcParameter.clear();
        if (jdbcParamInvocationList != null) {
            jdbcParamInvocationList.clear();
        }
    }

    @Override
    public void clearBatch() throws SQLException {
        if (preparedStatement != null) {
            preparedStatement.clearBatch();
        }
    }

    @Override
    public void addBatch() throws SQLException {
        initPreparedStatementIfAbsent();
        preparedStatement.addBatch();
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        if (preparedStatement != null) {
            super.addBatch(sql);
        } else {
            throw new UninitializedStatusException(
                                                   "Can't invoke 'addBatch(String sql)' before preparedStatement is initialized");
        }
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        if (preparedStatement != null) {
            return preparedStatement.getParameterMetaData();
        } else {
            throw new UninitializedStatusException(
                                                   "Can't invoke 'getParameterMetaData()' before preparedStatement is initialized");
        }
    }

    @Override
    public int getUpdateCount() throws SQLException {
        if (preparedStatement != null) {
            return preparedStatement.getUpdateCount();
        } else {
            throw new UninitializedStatusException(
                                                   "Can't invoke 'getUpdateCount()' before preparedStatement is initialized");
        }
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        if (preparedStatement != null) {
            return this.preparedStatement.getResultSetConcurrency();
        } else {
            throw new UninitializedStatusException(
                                                   "Can't invoke 'getResultSetConcurrency()' before preparedStatement is initialized");
        }
    }

    private void initPreparedStatementIfAbsent() throws SQLException {
        if (preparedStatement == null) {
            // 1. parse sql
            SQLParsedResult parsedResult = parseSql(sql, this.jdbcParameter);
            if (stdLogger.isDebugEnabled()) {
                stdLogger.debug(new StringBuilder("[ParseSql] from:")//
                .append(sql).append(" =>to: ")//
                .append(parsedResult.getSql()).toString());//
                if (stdLogger.isTraceEnabled()) {
                    stdLogger.trace("[JdbcParameter] " + DDRJSONUtils.toJSONString(jdbcParameter));
                }
            }
            this.sqlParsedResult = parsedResult;
            // 2. check if crossing datasource
            if (isCrossDataSource(parsedResult.getSchemas())) {
                throw new CrossDataSourceException("Current sql is using schemas:"
                                                   + DDRJSONUtils.toJSONString(parsedResult.getSchemas())
                                                   + ", but current datasource is bound on schemas:"
                                                   + DDRJSONUtils.toJSONString(schemas)
                                                   + ". Detail information: original sql is [" + sql
                                                   + "] and jdbc parameter is "
                                                   + DDRJSONUtils.toJSONString(jdbcParameter));
            }
            // 3. init preparedStatement if not
            DataSourceParam param = new DataSourceParam();
            param.setReadOnly(readOnly);
            param.setScNames(parsedResult.getSchemas());
            // 初始化statement
            try {// 记录关键信息
                initStatementIfAbsent(param, parsedResult.getSql());
            } catch (Throwable e) {
                throw new StatementInitializationException("readOnly:" + this.readOnly + " ,jdbc parameter:"
                                                           + DDRJSONUtils.toJSONString(this.jdbcParameter)
                                                           + " ,SQLParsedResult:" + parsedResult + " ,original sql:["
                                                           + sql + "]", e);
            }
            // 动作回放
            super.playbackInvocation(statement);
            playbackSetJdbcParamInvocation(preparedStatement, jdbcParamInvocationList);
        } else {// 同一个preparedStatement 以第一次成功创建preparedStatement为限制;
            this.sqlParsedResult.checkIfCrossPreparedStatement(this.jdbcParameter);
        }
    }

    @Override
    protected void initStatementIfAbsent(DataSourceParam param, String sql) throws SQLException {
        super.initStatementIfAbsent(param, sql);
        preparedStatement = (PreparedStatement) statement;
    }

    private enum JdbcParamSetMethod {

        setBoolean_boolean,

        setByte_byte,

        setShort_short,

        setInt_int,

        setLong_long,

        setFloat_float,

        setDouble_double,

        setTimestamp_Timestamp,

        setTimestamp_Timestamp_Calendar,

        setURL_URL,

        setTime_Time,

        setTime_Time_Calendar,

        setNull_int_String,

        setNull_int,

        setBigDecimal_BigDecimal,

        setString_String,

        setBytes_bytes,

        setDate_Date_Calendar,

        setDate_Date,

        setAsciiStream_InputStream_int,

        setAsciiStream_InputStream,

        setAsciiStream_InputStream_long,

        setUnicodeStream_InputStream_int,

        setBinaryStream_InputStream_int,

        setBinaryStream_InputStream_long,

        setBinaryStream_InputStream,

        setObject_Object_int_int,

        setObject_Object_int,

        setObject_Object,

        setCharacterStream_Reader_long,

        setCharacterStream_Reader_int,

        setCharacterStream_Reader,

        setRef_Ref,

        setBlob_InputStream_long,

        setBlob_InputStream,

        setBlob_Blob,

        setClob_Reader,

        setClob_Clob,

        setClob_Reader_long,

        setArray_Array,

        setRowId_RowId,

        setNString_String,

        setNCharacterStream_Reader,

        setNCharacterStream_Reader_long,

        setNClob_Reader,

        setNClob_Reader_long,

        setNClob_NClob,

        setSQLXML_SQLXML;
    }

    protected void playbackSetJdbcParamInvocation(PreparedStatement preparedStatement,
                                                  List<JdbcParamInvocation> jdbcParamInvocationList)
                                                                                                    throws SQLException {
        if (jdbcParamInvocationList == null || jdbcParamInvocationList.isEmpty()) {
            return;
        }
        for (JdbcParamInvocation item : jdbcParamInvocationList) {
            JdbcParamSetMethod method = item.getMethod();
            int index = item.getIndex();
            Object[] params = item.getParams();
            switch (method) {
                case setBoolean_boolean:
                    preparedStatement.setBoolean(index, (boolean) params[0]);
                    break;
                case setByte_byte:
                    preparedStatement.setByte(index, (byte) params[0]);
                    break;
                case setShort_short:
                    preparedStatement.setShort(index, (short) params[0]);
                    break;
                case setInt_int:
                    preparedStatement.setInt(index, (int) params[0]);
                    break;
                case setLong_long:
                    preparedStatement.setLong(index, (long) params[0]);
                    break;
                case setFloat_float:
                    preparedStatement.setFloat(index, (float) params[0]);
                    break;
                case setDouble_double:
                    preparedStatement.setDouble(index, (double) params[0]);
                    break;
                case setTimestamp_Timestamp:
                    preparedStatement.setTimestamp(index, (java.sql.Timestamp) params[0]);
                    break;
                case setTimestamp_Timestamp_Calendar:
                    preparedStatement.setTimestamp(index, (java.sql.Timestamp) params[0],
                                                   (java.util.Calendar) params[1]);
                    break;
                case setURL_URL:
                    preparedStatement.setURL(index, (java.net.URL) params[0]);
                    break;
                case setTime_Time_Calendar:
                    preparedStatement.setTime(index, (java.sql.Time) params[0], (java.util.Calendar) params[1]);
                    break;
                case setTime_Time:
                    preparedStatement.setTime(index, (java.sql.Time) params[0]);
                    break;
                case setArray_Array:
                    preparedStatement.setArray(index, (java.sql.Array) params[0]);
                    break;
                case setObject_Object_int:
                    preparedStatement.setObject(index, (java.lang.Object) params[0], (int) params[1]);
                    break;
                case setObject_Object_int_int:
                    preparedStatement.setObject(index, (java.lang.Object) params[0], (int) params[1], (int) params[2]);
                    break;
                case setObject_Object:
                    preparedStatement.setObject(index, (java.lang.Object) params[0]);
                    break;
                case setNull_int_String:
                    preparedStatement.setNull(index, (int) params[0], (java.lang.String) params[1]);
                    break;
                case setNull_int:
                    preparedStatement.setNull(index, (int) params[0]);
                    break;
                case setBigDecimal_BigDecimal:
                    preparedStatement.setBigDecimal(index, (java.math.BigDecimal) params[0]);
                    break;
                case setString_String:
                    preparedStatement.setString(index, (java.lang.String) params[0]);
                    break;
                case setBytes_bytes:
                    preparedStatement.setBytes(index, (byte[]) params[0]);
                    break;
                case setDate_Date_Calendar:
                    preparedStatement.setDate(index, (java.sql.Date) params[0], (java.util.Calendar) params[1]);
                    break;
                case setDate_Date:
                    preparedStatement.setDate(index, (java.sql.Date) params[0]);
                    break;
                case setAsciiStream_InputStream_int:
                    preparedStatement.setAsciiStream(index, (java.io.InputStream) params[0], (int) params[1]);
                    break;
                case setAsciiStream_InputStream_long:
                    preparedStatement.setAsciiStream(index, (java.io.InputStream) params[0], (long) params[1]);
                    break;
                case setAsciiStream_InputStream:
                    preparedStatement.setAsciiStream(index, (java.io.InputStream) params[0]);
                    break;
                case setUnicodeStream_InputStream_int:
                    preparedStatement.setUnicodeStream(index, (java.io.InputStream) params[0], (int) params[1]);
                    break;
                case setBinaryStream_InputStream_int:
                    preparedStatement.setBinaryStream(index, (java.io.InputStream) params[0], (int) params[1]);
                    break;
                case setBinaryStream_InputStream_long:
                    preparedStatement.setBinaryStream(index, (java.io.InputStream) params[0], (long) params[1]);
                    break;
                case setBinaryStream_InputStream:
                    preparedStatement.setBinaryStream(index, (java.io.InputStream) params[0]);
                    break;
                case setCharacterStream_Reader_int:
                    preparedStatement.setCharacterStream(index, (java.io.Reader) params[0], (int) params[1]);
                    break;
                case setCharacterStream_Reader_long:
                    preparedStatement.setCharacterStream(index, (java.io.Reader) params[0], (long) params[1]);
                    break;
                case setCharacterStream_Reader:
                    preparedStatement.setCharacterStream(index, (java.io.Reader) params[0]);
                    break;
                case setRef_Ref:
                    preparedStatement.setRef(index, (java.sql.Ref) params[0]);
                    break;
                case setBlob_InputStream_long:
                    preparedStatement.setBlob(index, (java.io.InputStream) params[0], (long) params[1]);
                    break;
                case setBlob_InputStream:
                    preparedStatement.setBlob(index, (java.io.InputStream) params[0]);
                    break;
                case setBlob_Blob:
                    preparedStatement.setBlob(index, (java.sql.Blob) params[0]);
                    break;
                case setClob_Reader:
                    preparedStatement.setClob(index, (java.io.Reader) params[0]);
                    break;
                case setClob_Reader_long:
                    preparedStatement.setClob(index, (java.io.Reader) params[0], (long) params[1]);
                    break;
                case setClob_Clob:
                    preparedStatement.setClob(index, (java.sql.Clob) params[0]);
                    break;
                case setRowId_RowId:
                    preparedStatement.setRowId(index, (java.sql.RowId) params[0]);
                    break;
                case setNString_String:
                    preparedStatement.setNString(index, (java.lang.String) params[0]);
                    break;
                case setNCharacterStream_Reader:
                    preparedStatement.setNCharacterStream(index, (java.io.Reader) params[0]);
                    break;
                case setNCharacterStream_Reader_long:
                    preparedStatement.setNCharacterStream(index, (java.io.Reader) params[0], (long) params[1]);
                    break;
                case setNClob_Reader_long:
                    preparedStatement.setNClob(index, (java.io.Reader) params[0], (long) params[1]);
                    break;
                case setNClob_NClob:
                    preparedStatement.setNClob(index, (java.sql.NClob) params[0]);
                    break;
                case setNClob_Reader:
                    preparedStatement.setNClob(index, (java.io.Reader) params[0]);
                    break;
                case setSQLXML_SQLXML:
                    preparedStatement.setSQLXML(index, (java.sql.SQLXML) params[0]);
                    break;
                default:
                    throw new UnsupportedPreparedStatementInvocationException("Unknown setter method '" + method + "'");
            }
        }
        jdbcParamInvocationList = null;
    }

    private void addJdbcParamInvokeRecord(JdbcParamSetMethod method, int index, Object[] params) {
        if (jdbcParamInvocationList == null) {
            jdbcParamInvocationList = new ArrayList<JdbcParamInvocation>();
        }
        jdbcParamInvocationList.add(new JdbcParamInvocation(method, index, params));
    }

    @Override
    public void setBoolean(int x0, boolean x1) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setBoolean(x0, x1);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setBoolean_boolean, x0, new Object[] { x1 });
        }
    }

    @Override
    public void setByte(int x0, byte x1) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setByte(x0, x1);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setByte_byte, x0, new Object[] { x1 });
        }
    }

    @Override
    public void setShort(int x0, short x1) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setShort(x0, x1);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setShort_short, x0, new Object[] { x1 });
        }
    }

    @Override
    public void setInt(int x0, int x1) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setInt(x0, x1);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setInt_int, x0, new Object[] { x1 });
        }
    }

    @Override
    public void setLong(int x0, long x1) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setLong(x0, x1);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setLong_long, x0, new Object[] { x1 });
        }
    }

    @Override
    public void setFloat(int x0, float x1) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setFloat(x0, x1);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setFloat_float, x0, new Object[] { x1 });
        }
    }

    @Override
    public void setDouble(int x0, double x1) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setDouble(x0, x1);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setDouble_double, x0, new Object[] { x1 });
        }
    }

    @Override
    public void setTimestamp(int x0, java.sql.Timestamp x1) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setTimestamp(x0, x1);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setTimestamp_Timestamp, x0, new Object[] { x1 });
        }
    }

    @Override
    public void setTimestamp(int x0, java.sql.Timestamp x1, java.util.Calendar x2) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setTimestamp(x0, x1, x2);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setTimestamp_Timestamp_Calendar, x0, new Object[] { x1, x2 });
        }
    }

    @Override
    public void setURL(int x0, java.net.URL x1) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setURL(x0, x1);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setURL_URL, x0, new Object[] { x1 });
        }
    }

    @Override
    public void setTime(int x0, java.sql.Time x1) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setTime(x0, x1);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setTime_Time, x0, new Object[] { x1 });
        }
    }

    @Override
    public void setTime(int x0, java.sql.Time x1, java.util.Calendar x2) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setTime(x0, x1, x2);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setTime_Time_Calendar, x0, new Object[] { x1, x2 });
        }
    }

    @Override
    public void setNull(int x0, int x1, java.lang.String x2) throws SQLException {
        jdbcParameter.put(x0, null);
        if (preparedStatement != null) {
            preparedStatement.setNull(x0, x1, x2);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setNull_int_String, x0, new Object[] { x1, x2 });
        }
    }

    @Override
    public void setNull(int x0, int x1) throws SQLException {
        jdbcParameter.put(x0, null);
        if (preparedStatement != null) {
            preparedStatement.setNull(x0, x1);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setNull_int, x0, new Object[] { x1 });
        }
    }

    @Override
    public void setBigDecimal(int x0, java.math.BigDecimal x1) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setBigDecimal(x0, x1);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setBigDecimal_BigDecimal, x0, new Object[] { x1 });
        }
    }

    @Override
    public void setString(int x0, java.lang.String x1) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setString(x0, x1);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setString_String, x0, new Object[] { x1 });
        }
    }

    @Override
    public void setBytes(int x0, byte[] x1) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setBytes(x0, x1);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setBytes_bytes, x0, new Object[] { x1 });
        }
    }

    @Override
    public void setDate(int x0, java.sql.Date x1, java.util.Calendar x2) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setDate(x0, x1, x2);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setDate_Date_Calendar, x0, new Object[] { x1, x2 });
        }
    }

    @Override
    public void setDate(int x0, java.sql.Date x1) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setDate(x0, x1);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setDate_Date, x0, new Object[] { x1 });
        }
    }

    @Override
    public void setAsciiStream(int x0, java.io.InputStream x1, int x2) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setAsciiStream(x0, x1, x2);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setAsciiStream_InputStream_int, x0, new Object[] { x1, x2 });
        }
    }

    @Override
    public void setAsciiStream(int x0, java.io.InputStream x1) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setAsciiStream(x0, x1);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setAsciiStream_InputStream, x0, new Object[] { x1 });
        }
    }

    @Override
    public void setAsciiStream(int x0, java.io.InputStream x1, long x2) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setAsciiStream(x0, x1, x2);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setAsciiStream_InputStream_long, x0, new Object[] { x1, x2 });
        }
    }

    @Override
    public void setUnicodeStream(int x0, java.io.InputStream x1, int x2) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setUnicodeStream(x0, x1, x2);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setUnicodeStream_InputStream_int, x0, new Object[] { x1, x2 });
        }
    }

    @Override
    public void setBinaryStream(int x0, java.io.InputStream x1, int x2) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setBinaryStream(x0, x1, x2);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setBinaryStream_InputStream_int, x0, new Object[] { x1, x2 });
        }
    }

    @Override
    public void setBinaryStream(int x0, java.io.InputStream x1, long x2) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setBinaryStream(x0, x1, x2);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setBinaryStream_InputStream_long, x0, new Object[] { x1, x2 });
        }
    }

    @Override
    public void setBinaryStream(int x0, java.io.InputStream x1) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setBinaryStream(x0, x1);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setBinaryStream_InputStream, x0, new Object[] { x1 });
        }
    }

    @Override
    public void setObject(int x0, java.lang.Object x1, int x2, int x3) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setObject(x0, x1, x2, x3);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setObject_Object_int_int, x0, new Object[] { x1, x2, x3 });
        }
    }

    @Override
    public void setObject(int x0, java.lang.Object x1, int x2) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setObject(x0, x1, x2);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setObject_Object_int, x0, new Object[] { x1, x2 });
        }
    }

    @Override
    public void setObject(int x0, java.lang.Object x1) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setObject(x0, x1);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setObject_Object, x0, new Object[] { x1 });
        }
    }

    @Override
    public void setCharacterStream(int x0, java.io.Reader x1, long x2) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setCharacterStream(x0, x1, x2);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setCharacterStream_Reader_long, x0, new Object[] { x1, x2 });
        }
    }

    @Override
    public void setCharacterStream(int x0, java.io.Reader x1, int x2) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setCharacterStream(x0, x1, x2);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setCharacterStream_Reader_int, x0, new Object[] { x1, x2 });
        }
    }

    @Override
    public void setCharacterStream(int x0, java.io.Reader x1) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setCharacterStream(x0, x1);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setCharacterStream_Reader, x0, new Object[] { x1 });
        }
    }

    @Override
    public void setRef(int x0, java.sql.Ref x1) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setRef(x0, x1);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setRef_Ref, x0, new Object[] { x1 });
        }
    }

    @Override
    public void setBlob(int x0, java.io.InputStream x1, long x2) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setBlob(x0, x1, x2);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setBlob_InputStream_long, x0, new Object[] { x1, x2 });
        }
    }

    @Override
    public void setBlob(int x0, java.io.InputStream x1) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setBlob(x0, x1);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setBlob_InputStream, x0, new Object[] { x1 });
        }
    }

    @Override
    public void setBlob(int x0, java.sql.Blob x1) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setBlob(x0, x1);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setBlob_Blob, x0, new Object[] { x1 });
        }
    }

    @Override
    public void setClob(int x0, java.io.Reader x1) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setClob(x0, x1);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setClob_Reader, x0, new Object[] { x1 });
        }
    }

    @Override
    public void setClob(int x0, java.sql.Clob x1) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setClob(x0, x1);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setClob_Clob, x0, new Object[] { x1 });
        }
    }

    @Override
    public void setClob(int x0, java.io.Reader x1, long x2) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setClob(x0, x1, x2);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setClob_Reader_long, x0, new Object[] { x1, x2 });
        }
    }

    @Override
    public void setArray(int x0, java.sql.Array x1) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setArray(x0, x1);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setArray_Array, x0, new Object[] { x1 });
        }
    }

    @Override
    public void setRowId(int x0, java.sql.RowId x1) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setRowId(x0, x1);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setRowId_RowId, x0, new Object[] { x1 });
        }
    }

    @Override
    public void setNString(int x0, java.lang.String x1) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setNString(x0, x1);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setNString_String, x0, new Object[] { x1 });
        }
    }

    @Override
    public void setNCharacterStream(int x0, java.io.Reader x1) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setNCharacterStream(x0, x1);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setNCharacterStream_Reader, x0, new Object[] { x1 });
        }
    }

    @Override
    public void setNCharacterStream(int x0, java.io.Reader x1, long x2) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setNCharacterStream(x0, x1, x2);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setNCharacterStream_Reader_long, x0, new Object[] { x1, x2 });
        }
    }

    @Override
    public void setNClob(int x0, java.io.Reader x1) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setNClob(x0, x1);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setNClob_Reader, x0, new Object[] { x1 });
        }
    }

    @Override
    public void setNClob(int x0, java.io.Reader x1, long x2) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setNClob(x0, x1, x2);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setNClob_Reader_long, x0, new Object[] { x1, x2 });
        }
    }

    @Override
    public void setNClob(int x0, java.sql.NClob x1) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setNClob(x0, x1);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setNClob_NClob, x0, new Object[] { x1 });
        }
    }

    @Override
    public void setSQLXML(int x0, java.sql.SQLXML x1) throws SQLException {
        jdbcParameter.put(x0, x1);
        if (preparedStatement != null) {
            preparedStatement.setSQLXML(x0, x1);
        } else {
            addJdbcParamInvokeRecord(JdbcParamSetMethod.setSQLXML_SQLXML, x0, new Object[] { x1 });
        }
    }

    // NOTE:特殊处理
    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        if (preparedStatement != null) {
            return preparedStatement.getMetaData();
        } else {
            return null;
        }
    }
}
