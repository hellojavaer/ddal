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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.logging.Logger;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">zoukaiming[邹凯明]</a>,created on 05/11/2016.
 */
public abstract class AbstarctDDRDateSource implements DataSource {

    protected final Log logger = LogFactory.getLog(getClass());

    protected abstract String replaceSql(String sql, Map<Integer, Object> jdbcParam);

    protected abstract DataSource getDataSource();

    @Override
    public int getLoginTimeout() throws SQLException {
        return 0;
    }

    @Override
    public void setLoginTimeout(int timeout) throws SQLException {
        throw new UnsupportedOperationException("setLoginTimeout");
    }

    @Override
    public PrintWriter getLogWriter() {
        throw new UnsupportedOperationException("getLogWriter");
    }

    @Override
    public void setLogWriter(PrintWriter pw) throws SQLException {
        throw new UnsupportedOperationException("setLogWriter");
    }

    @Override
    @SuppressWarnings("unchecked")
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

    @Override
    public Logger getParentLogger() {
        return Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return new ConnectionWrapper(getDataSource().getConnection());
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return new ConnectionWrapper(getDataSource().getConnection());
    }

    private class ConnectionWrapper implements Connection {

        private Connection connection;

        public ConnectionWrapper(Connection connection) {
            this.connection = connection;
        }

        @Override
        public Statement createStatement() throws SQLException {
            return new StatementWrapper(this.connection, null, "createStatement", new Object[] {}, new Class[]{}) {

                @Override
                public String replaceSql(String sql, Map<Integer, Object> jdbcParams) {
                    return AbstarctDDRDateSource.this.replaceSql(sql, jdbcParams);
                }
            };
        }

        @Override
        public PreparedStatement prepareStatement(String sql) throws SQLException {
            return new StatementWrapper(this.connection, sql, "prepareStatement", new Object[] { sql }, new Class[]{String.class}) {

                @Override
                public String replaceSql(String sql, Map<Integer, Object> jdbcParams) {
                    return AbstarctDDRDateSource.this.replaceSql(sql, jdbcParams);
                }
            };
        }

        @Override
        public CallableStatement prepareCall(String sql) throws SQLException {
            throw new UnsupportedOperationException("not support prepareCall");
        }

        @Override
        public String nativeSQL(String sql) throws SQLException {
            return connection.nativeSQL(sql);
        }

        @Override
        public void setAutoCommit(boolean autoCommit) throws SQLException {
            connection.setAutoCommit(autoCommit);
        }

        @Override
        public boolean getAutoCommit() throws SQLException {
            return connection.getAutoCommit();
        }

        @Override
        public void commit() throws SQLException {
            connection.commit();
        }

        @Override
        public void rollback() throws SQLException {
            connection.rollback();
        }

        @Override
        public void close() throws SQLException {
            connection.close();
        }

        @Override
        public boolean isClosed() throws SQLException {
            return connection.isClosed();
        }

        @Override
        public DatabaseMetaData getMetaData() throws SQLException {
            return connection.getMetaData();
        }

        @Override
        public void setReadOnly(boolean readOnly) throws SQLException {
            connection.setReadOnly(readOnly);
        }

        @Override
        public boolean isReadOnly() throws SQLException {
            return connection.isReadOnly();
        }

        @Override
        public void setCatalog(String catalog) throws SQLException {
            connection.setCatalog(catalog);
        }

        @Override
        public String getCatalog() throws SQLException {
            return connection.getCatalog();
        }

        @Override
        public void setTransactionIsolation(int level) throws SQLException {
            connection.setTransactionIsolation(level);
        }

        @Override
        public int getTransactionIsolation() throws SQLException {
            return connection.getTransactionIsolation();
        }

        @Override
        public SQLWarning getWarnings() throws SQLException {
            return connection.getWarnings();
        }

        @Override
        public void clearWarnings() throws SQLException {
            connection.clearWarnings();
        }

        @Override
        public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
            return new StatementWrapper(this.connection, null, "createStatement", new Object[] { resultSetType,
                    resultSetConcurrency }, new Class[]{int.class, int.class}) {

                @Override
                public String replaceSql(String sql, Map<Integer, Object> jdbcParams) {
                    return AbstarctDDRDateSource.this.replaceSql(sql, jdbcParams);
                }
            };
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
                                                                                                          throws SQLException {
            return new StatementWrapper(this.connection, sql, "prepareStatement", new Object[] { sql, resultSetType,
                    resultSetConcurrency }, new Class[]{String.class, int.class, int.class}) {

                @Override
                public String replaceSql(String sql, Map<Integer, Object> jdbcParams) {
                    return AbstarctDDRDateSource.this.replaceSql(sql, jdbcParams);
                }
            };
        }

        @Override
        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
                                                                                                     throws SQLException {
            throw new UnsupportedOperationException("prepareCall");
        }

        @Override
        public Map<String, Class<?>> getTypeMap() throws SQLException {
            return connection.getTypeMap();
        }

        @Override
        public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
            connection.setTypeMap(map);
        }

        @Override
        public void setHoldability(int holdability) throws SQLException {
            connection.setHoldability(holdability);
        }

        @Override
        public int getHoldability() throws SQLException {
            return connection.getHoldability();
        }

        @Override
        public Savepoint setSavepoint() throws SQLException {
            return connection.setSavepoint();
        }

        @Override
        public Savepoint setSavepoint(String name) throws SQLException {
            return connection.setSavepoint(name);
        }

        @Override
        public void rollback(Savepoint savepoint) throws SQLException {
            connection.rollback(savepoint);
        }

        @Override
        public void releaseSavepoint(Savepoint savepoint) throws SQLException {
            connection.releaseSavepoint(savepoint);
        }

        @Override
        public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
                                                                                                               throws SQLException {
            return new StatementWrapper(this.connection, null, "createStatement", new Object[] { resultSetType,
                    resultSetConcurrency, resultSetHoldability }, new Class[]{int.class,int.class,int.class}) {

                @Override
                public String replaceSql(String sql, Map<Integer, Object> jdbcParams) {
                    return AbstarctDDRDateSource.this.replaceSql(sql, jdbcParams);
                }
            };
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                                  int resultSetHoldability) throws SQLException {
            return new StatementWrapper(this.connection, sql, "prepareStatement", new Object[] { sql, resultSetType,
                    resultSetConcurrency, resultSetHoldability },new Class[]{String.class, int.class,int.class,int.class}) {

                @Override
                public String replaceSql(String sql, Map<Integer, Object> jdbcParams) {
                    return AbstarctDDRDateSource.this.replaceSql(sql, jdbcParams);
                }
            };
        }

        @Override
        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                             int resultSetHoldability) throws SQLException {
            throw new UnsupportedOperationException("not support prepareCall");
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
            return new StatementWrapper(this.connection, sql, "prepareStatement",
                                        new Object[] { sql, autoGeneratedKeys },new Class[]{String.class,int.class}) {

                @Override
                public String replaceSql(String sql, Map<Integer, Object> jdbcParams) {
                    return AbstarctDDRDateSource.this.replaceSql(sql, jdbcParams);
                }
            };
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
            return new StatementWrapper(this.connection, sql, "prepareStatement", new Object[] { sql, columnIndexes },new Class[]{String.class, int[].class}) {

                @Override
                public String replaceSql(String sql, Map<Integer, Object> jdbcParams) {
                    return AbstarctDDRDateSource.this.replaceSql(sql, jdbcParams);
                }
            };
        }

        @Override
        public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
            return new StatementWrapper(this.connection, sql, "prepareStatement", new Object[] { sql, columnNames },new Class[]{String.class, String[].class}) {

                @Override
                public String replaceSql(String sql, Map<Integer, Object> jdbcParams) {
                    return AbstarctDDRDateSource.this.replaceSql(sql, jdbcParams);
                }
            };
        }

        @Override
        public Clob createClob() throws SQLException {
            return connection.createClob();
        }

        @Override
        public Blob createBlob() throws SQLException {
            return connection.createBlob();
        }

        @Override
        public NClob createNClob() throws SQLException {
            return connection.createNClob();
        }

        @Override
        public SQLXML createSQLXML() throws SQLException {
            return null;
        }

        @Override
        public boolean isValid(int timeout) throws SQLException {
            return connection.isValid(timeout);
        }

        @Override
        public void setClientInfo(String name, String value) throws SQLClientInfoException {
            connection.setClientInfo(name, value);
        }

        @Override
        public void setClientInfo(Properties properties) throws SQLClientInfoException {
            connection.setClientInfo(properties);
        }

        @Override
        public String getClientInfo(String name) throws SQLException {
            return connection.getClientInfo(name);
        }

        @Override
        public Properties getClientInfo() throws SQLException {
            return connection.getClientInfo();
        }

        @Override
        public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
            return connection.createArrayOf(typeName, elements);
        }

        @Override
        public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
            return connection.createStruct(typeName, attributes);
        }

        @Override
        public void setSchema(String schema) throws SQLException {
            connection.setSchema(schema);
        }

        @Override
        public String getSchema() throws SQLException {
            return connection.getSchema();
        }

        @Override
        public void abort(Executor executor) throws SQLException {
            connection.abort(executor);
        }

        @Override
        public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
            connection.setNetworkTimeout(executor, milliseconds);
        }

        @Override
        public int getNetworkTimeout() throws SQLException {
            return connection.getNetworkTimeout();
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            return connection.unwrap(iface);
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return connection.isWrapperFor(iface);
        }
    }

}
