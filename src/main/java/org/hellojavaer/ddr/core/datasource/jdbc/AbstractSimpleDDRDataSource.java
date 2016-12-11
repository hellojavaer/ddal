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

import org.hellojavaer.ddr.core.datasource.DataSourceSchemasBinding;
import org.hellojavaer.ddr.core.datasource.manager.DataSourceParam;
import org.hellojavaer.ddr.core.datasource.tr.TransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">zoukaiming[邹凯明]</a>,created on 09/12/2016.
 */
public abstract class AbstractSimpleDDRDataSource implements DDRDataSource {

    protected final Logger           logger = LoggerFactory.getLogger(getClass());

    private boolean                  readOnly;
    private DataSourceSchemasBinding dataSourceSchemasBinding;

    private boolean isCrossDataSource0(Set<String> schemas) {
        if (dataSourceSchemasBinding == null) {
            throw null;// TODO
        } else {
            return dataSourceSchemasBinding.getSchemas().containsAll(schemas);
        }
    }

    private DataSource getDataSource0() {
        if (dataSourceSchemasBinding == null) {
            synchronized (this) {
                if (dataSourceSchemasBinding == null) {
                    DataSourceParam param = new DataSourceParam();
                    readOnly = TransactionManager.isReadOnly();
                    param.setReadOnly(readOnly);
                    dataSourceSchemasBinding = getDataSource(param);
                    if (dataSourceSchemasBinding == null) {
                        throw new IllegalStateException(
                                                        "[SimpleDDRDataSource] no datasource is configured for readOnly:"
                                                                + readOnly);
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug("[SimpleDDRDataSource] initialize datasource with readOnly:" + readOnly);
                    }
                }
            }
        }
        return dataSourceSchemasBinding.getDataSource();
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return getDataSource0().getParentLogger();
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return getDataSource0().getLoginTimeout();
    }

    @Override
    public void setLoginTimeout(int timeout) throws SQLException {
        getDataSource0().setLoginTimeout(timeout);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return getDataSource0().getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter pw) throws SQLException {
        getDataSource0().setLogWriter(pw);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return getDataSource0().unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return getDataSource0().isWrapperFor(iface);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return new ConnectionWrapper(getDataSource0().getConnection(), readOnly);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return new ConnectionWrapper(getDataSource0().getConnection(), readOnly);
    }

    private class ConnectionWrapper implements Connection {

        private boolean    readOnly;
        private Connection connection;

        private boolean isReadOnly0() {
            return readOnly;
        }

        private Connection getConnection0() {
            return connection;
        }

        public ConnectionWrapper(Connection connection, boolean readOnly) {
            this.connection = connection;
            this.readOnly = readOnly;
        }

        @Override
        public Statement createStatement() throws SQLException {
            return new StatementWrapper(isReadOnly0()) {

                @Override
                public DDRDataSource.ReplacedResult replaceSql(String sql, Map<Integer, Object> jdbcParams) {
                    return AbstractSimpleDDRDataSource.this.replaceSql(sql, jdbcParams);
                }

                @Override
                public boolean isCrossDataSource(Set<String> schemas) {
                    return isCrossDataSource0(schemas);
                }

                @Override
                public Statement getStatement(DataSourceParam param) throws SQLException {
                    return null;
                }
            };
        }

        @Override
        public PreparedStatement prepareStatement(final String sql) throws SQLException {
            return new PreparedStatementWrapper(sql, isReadOnly0()) {

                @Override
                public DDRDataSource.ReplacedResult replaceSql(String sql, Map<Integer, Object> jdbcParams) {
                    return replaceSql(sql, jdbcParams);
                }

                @Override
                public boolean isCrossDataSource(Set<String> schemas) {
                    return isCrossDataSource0(schemas);
                }

                @Override
                public PreparedStatement getStatement(DataSourceParam param, String routedSql) {
                    try {
                        return getConnection0().prepareStatement(routedSql);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
        }

        @Override
        public CallableStatement prepareCall(String sql) throws SQLException {
            throw new UnsupportedOperationException("prepareCall");
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
            return new StatementWrapper(isReadOnly0()) {

                @Override
                public DDRDataSource.ReplacedResult replaceSql(String sql, Map<Integer, Object> jdbcParams) {
                    return AbstractSimpleDDRDataSource.this.replaceSql(sql, jdbcParams);
                }

                @Override
                public boolean isCrossDataSource(Set<String> schemas) {
                    return isCrossDataSource0(schemas);
                }

                @Override
                public Statement getStatement(DataSourceParam param) throws SQLException {
                    return null;
                }
            };
        }

        @Override
        public PreparedStatement prepareStatement(String sql, final int resultSetType, final int resultSetConcurrency)
                                                                                                                      throws SQLException {
            return new PreparedStatementWrapper(sql, isReadOnly0()) {

                @Override
                public DDRDataSource.ReplacedResult replaceSql(String sql, Map<Integer, Object> jdbcParams) {
                    return replaceSql(sql, jdbcParams);
                }

                @Override
                public boolean isCrossDataSource(Set<String> schemas) {
                    return isCrossDataSource0(schemas);
                }

                @Override
                public PreparedStatement getStatement(DataSourceParam param, String routedSql) {
                    try {
                        return getConnection0().prepareStatement(routedSql, resultSetType, resultSetConcurrency);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
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
            return new StatementWrapper(isReadOnly0()) {

                @Override
                public DDRDataSource.ReplacedResult replaceSql(String sql, Map<Integer, Object> jdbcParams) {
                    return AbstractSimpleDDRDataSource.this.replaceSql(sql, jdbcParams);
                }

                @Override
                public boolean isCrossDataSource(Set<String> schemas) {
                    return isCrossDataSource0(schemas);
                }

                @Override
                public Statement getStatement(DataSourceParam param) throws SQLException {
                    return null;
                }
            };
        }

        @Override
        public PreparedStatement prepareStatement(String sql, final int resultSetType, final int resultSetConcurrency,
                                                  final int resultSetHoldability) throws SQLException {
            return new PreparedStatementWrapper(sql, isReadOnly0()) {

                @Override
                public DDRDataSource.ReplacedResult replaceSql(String sql, Map<Integer, Object> jdbcParams) {
                    return replaceSql(sql, jdbcParams);
                }

                @Override
                public boolean isCrossDataSource(Set<String> schemas) {
                    return isCrossDataSource0(schemas);
                }

                @Override
                public PreparedStatement getStatement(DataSourceParam param, String routedSql) {
                    try {
                        return getConnection0().prepareStatement(routedSql, resultSetType, resultSetConcurrency,
                                                                 resultSetHoldability);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
        }

        @Override
        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                             int resultSetHoldability) throws SQLException {
            throw new UnsupportedOperationException("prepareCall");
        }

        @Override
        public PreparedStatement prepareStatement(String sql, final int autoGeneratedKeys) throws SQLException {
            return new PreparedStatementWrapper(sql, isReadOnly0()) {

                @Override
                public DDRDataSource.ReplacedResult replaceSql(String sql, Map<Integer, Object> jdbcParams) {
                    return replaceSql(sql, jdbcParams);
                }

                @Override
                public boolean isCrossDataSource(Set<String> schemas) {
                    return isCrossDataSource0(schemas);
                }

                @Override
                public PreparedStatement getStatement(DataSourceParam param, String routedSql) {
                    try {
                        return getConnection0().prepareStatement(routedSql, autoGeneratedKeys);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
        }

        @Override
        public PreparedStatement prepareStatement(String sql, final int[] columnIndexes) throws SQLException {
            return new PreparedStatementWrapper(sql, isReadOnly0()) {

                @Override
                public DDRDataSource.ReplacedResult replaceSql(String sql, Map<Integer, Object> jdbcParams) {
                    return replaceSql(sql, jdbcParams);
                }

                @Override
                public boolean isCrossDataSource(Set<String> schemas) {
                    return isCrossDataSource0(schemas);
                }

                @Override
                public PreparedStatement getStatement(DataSourceParam param, String routedSql) {
                    try {
                        return getConnection0().prepareStatement(routedSql, columnIndexes);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
        }

        @Override
        public PreparedStatement prepareStatement(String sql, final String[] columnNames) throws SQLException {
            return new PreparedStatementWrapper(sql, isReadOnly0()) {

                @Override
                public DDRDataSource.ReplacedResult replaceSql(String sql, Map<Integer, Object> jdbcParams) {
                    return replaceSql(sql, jdbcParams);
                }

                @Override
                public boolean isCrossDataSource(Set<String> schemas) {
                    return isCrossDataSource0(schemas);
                }

                @Override
                public PreparedStatement getStatement(DataSourceParam param, String routedSql) {
                    try {
                        return getConnection0().prepareStatement(routedSql, columnNames);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
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
