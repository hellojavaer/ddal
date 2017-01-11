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

import org.hellojavaer.ddal.ddr.datasource.exception.UninitializedStatusException;
import org.hellojavaer.ddal.ddr.datasource.exception.UnsupportedConnectionInvocationException;
import org.hellojavaer.ddal.ddr.datasource.jdbc.init.UninitializedDataSourceProcessor;
import org.hellojavaer.ddal.ddr.datasource.jdbc.property.ConnectionProperty;
import org.hellojavaer.ddal.ddr.datasource.jdbc.property.DataSourceProperty;
import org.hellojavaer.ddal.ddr.datasource.manager.DataSourceParam;
import org.hellojavaer.ddal.ddr.datasource.exception.DataSourceNotFoundException;
import org.hellojavaer.ddal.ddr.datasource.exception.UnsupportedDataSourceInvocationException;
import org.hellojavaer.ddal.ddr.datasource.jdbc.init.UninitializedConnectionProcessor;
import org.hellojavaer.ddal.ddr.sqlparse.SQLParsedResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 05/11/2016.
 */
public abstract class AbstractDDRDataSource implements DDRDataSource {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private DataSourceWrapper getDataSource0(DataSourceParam param) throws SQLException {
        DataSourceWrapper dataSourceWrapper = this.getDataSource(param);
        if (dataSourceWrapper == null) {
            throw new DataSourceNotFoundException("No datasource found for parameter:" + param.toString());
        } else {
            return dataSourceWrapper;
        }
    }

    @Override
    public java.util.logging.Logger getParentLogger() {
        return java.util.logging.Logger.getLogger(java.util.logging.Logger.GLOBAL_LOGGER_NAME);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        if (UninitializedDataSourceProcessor.isSetDefaultValue(DataSourceProperty.loginTimeout)) {
            int val = ((Number) UninitializedDataSourceProcessor.getDefaultValue(DataSourceProperty.loginTimeout)).intValue();
            return val;
        } else {
            throw new UninitializedStatusException(
                                                   "Can't invoke 'getLoginTimeout()' when 'loginTimeout' isn't set default value");
        }
    }

    @Override
    public void setLoginTimeout(int timeout) throws SQLException {
        throw new UnsupportedDataSourceInvocationException("setLoginTimeout");
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        if (UninitializedDataSourceProcessor.isSetDefaultValue(DataSourceProperty.logWriter)) {
            PrintWriter val = (PrintWriter) UninitializedDataSourceProcessor.getDefaultValue(DataSourceProperty.logWriter);
            return val;
        } else {
            throw new UninitializedStatusException(
                                                   "Can't invoke 'getLogWriter()' when 'logWriter' isn't set default value");
        }
    }

    @Override
    public void setLogWriter(PrintWriter pw) throws SQLException {
        throw new UnsupportedDataSourceInvocationException("setLogWriter");
    }

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

    @Override
    public Connection getConnection() throws SQLException {
        return new ConnectionWrapper() {

            @Override
            public ConnectionResult getConnection(DataSourceParam param) throws SQLException {
                DataSourceWrapper dataSourceWrapper = getDataSource0(param);
                return new ConnectionResult(dataSourceWrapper.getDataSource().getConnection(),
                                            dataSourceWrapper.getSchemas());
            }
        };
    }

    @Override
    public Connection getConnection(final String username, final String password) throws SQLException {
        return new ConnectionWrapper() {

            @Override
            public ConnectionResult getConnection(DataSourceParam param) throws SQLException {
                DataSourceWrapper dataSourceWrapper = getDataSource0(param);
                return new ConnectionResult(dataSourceWrapper.getDataSource().getConnection(username, password),
                                            dataSourceWrapper.getSchemas());
            }
        };
    }

    private class ConnectionResult {

        private Connection  connection;
        private Set<String> schemas;

        public ConnectionResult(Connection connection, Set<String> schemas) {
            this.connection = connection;
            this.schemas = schemas;
        }

        public Connection getConnection() {
            return connection;
        }

        public void setConnection(Connection connection) {
            this.connection = connection;
        }

        public Set<String> getSchemas() {
            return schemas;
        }

        public void setSchemas(Set<String> schemas) {
            this.schemas = schemas;
        }
    }

    private abstract class ConnectionWrapper implements Connection {

        private class ConnectionPropertyBean {

            private boolean               readOnly;
            private boolean               autoCommit;
            private String                catalog;
            private int                   transactionIsolation;
            private Map<String, Class<?>> typeMap;
            private int                   holdability;
            private String                schema;
            private DatabaseMetaData      metaData;

            public boolean isReadOnly() {
                return readOnly;
            }

            public void setReadOnly(boolean readOnly) {
                this.readOnly = readOnly;
            }

            public boolean isAutoCommit() {
                return autoCommit;
            }

            public void setAutoCommit(boolean autoCommit) {
                this.autoCommit = autoCommit;
            }

            public String getCatalog() {
                return catalog;
            }

            public void setCatalog(String catalog) {
                this.catalog = catalog;
            }

            public int getTransactionIsolation() {
                return transactionIsolation;
            }

            public void setTransactionIsolation(int transactionIsolation) {
                this.transactionIsolation = transactionIsolation;
            }

            public Map<String, Class<?>> getTypeMap() {
                return typeMap;
            }

            public void setTypeMap(Map<String, Class<?>> typeMap) {
                this.typeMap = typeMap;
            }

            public int getHoldability() {
                return holdability;
            }

            public void setHoldability(int holdability) {
                this.holdability = holdability;
            }

            public String getSchema() {
                return schema;
            }

            public void setSchema(String schema) {
                this.schema = schema;
            }

            public DatabaseMetaData getMetaData() {
                return metaData;
            }

            public void setMetaData(DatabaseMetaData metaData) {
                this.metaData = metaData;
            }
        }

        private class InvocationTag {

            private boolean readOnly;
            private boolean autoCommit;
            private boolean catalog;
            private boolean transactionIsolation;
            private boolean typeMap;
            private boolean holdability;
            private boolean schema;
            private boolean metaData;

            public boolean isReadOnly() {
                return readOnly;
            }

            public void setReadOnly(boolean readOnly) {
                this.readOnly = readOnly;
            }

            public boolean isAutoCommit() {
                return autoCommit;
            }

            public void setAutoCommit(boolean autoCommit) {
                this.autoCommit = autoCommit;
            }

            public boolean isCatalog() {
                return catalog;
            }

            public void setCatalog(boolean catalog) {
                this.catalog = catalog;
            }

            public boolean isTransactionIsolation() {
                return transactionIsolation;
            }

            public void setTransactionIsolation(boolean transactionIsolation) {
                this.transactionIsolation = transactionIsolation;
            }

            public boolean isTypeMap() {
                return typeMap;
            }

            public void setTypeMap(boolean typeMap) {
                this.typeMap = typeMap;
            }

            public boolean isHoldability() {
                return holdability;
            }

            public void setHoldability(boolean holdability) {
                this.holdability = holdability;
            }

            public boolean isSchema() {
                return schema;
            }

            public void setSchema(boolean schema) {
                this.schema = schema;
            }

            public boolean isMetaData() {
                return metaData;
            }

            public void setMetaData(boolean metaData) {
                this.metaData = metaData;
            }
        }

        //
        private volatile ConnectionResult       connectionResult;

        private volatile ConnectionPropertyBean prop = new ConnectionPropertyBean();
        private volatile InvocationTag          tag  = new InvocationTag();

        private boolean isReadOnly0() {
            return prop.isReadOnly();
        }

        private Set<String> getSchemas0() {
            if (connectionResult == null) {
                return null;
            } else {
                return connectionResult.getSchemas();
            }
        }

        public abstract ConnectionResult getConnection(DataSourceParam param) throws SQLException;

        private Connection getConnection1() {
            if (connectionResult == null) {
                return null;
            } else {
                return connectionResult.getConnection();
            }
        }

        private void closeConnection0(Connection connection) {
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception e) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("[closeConnection]", e);
                    }
                }
            }
        }

        private ConnectionResult getConnection0(DataSourceParam param) throws SQLException {
            if (this.connectionResult == null || this.connectionResult.getConnection().getAutoCommit()) {
                if (this.connectionResult != null && this.connectionResult.getConnection() != null) {
                    closeConnection0(this.connectionResult.getConnection());
                }
                ConnectionResult connectionResult = getConnection(param);
                Connection connection = connectionResult.getConnection();
                // playback
                if (tag.isAutoCommit()) {
                    connection.setAutoCommit(prop.isAutoCommit());
                }
                if (tag.isReadOnly()) {
                    connection.setReadOnly(prop.isReadOnly());
                }
                if (tag.isSchema()) {
                    connection.setSchema(prop.getSchema());
                }
                if (tag.isTypeMap()) {
                    connection.setTypeMap(prop.getTypeMap());
                }
                if (tag.isTransactionIsolation()) {
                    connection.setTransactionIsolation(prop.getTransactionIsolation());
                }
                if (tag.isHoldability()) {
                    connection.setHoldability(prop.getHoldability());
                }
                if (tag.isCatalog()) {
                    connection.setCatalog(prop.getCatalog());
                }
                this.connectionResult = connectionResult;
            }
            return connectionResult;
        }

        @Override
        public Statement createStatement() throws SQLException {
            return new DDRStatementImpl(isReadOnly0(), getSchemas0()) {

                @Override
                public SQLParsedResult parseSql(String sql, Map<Object, Object> jdbcParams) throws SQLException {
                    return AbstractDDRDataSource.this.parseSql(sql, jdbcParams);
                }

                @Override
                public synchronized StatementWrapper getStatement(DataSourceParam param, String sql)
                                                                                                    throws SQLException {
                    ConnectionResult connectionResult = getConnection0(param);
                    Statement statement = connectionResult.getConnection().createStatement();
                    return new StatementWrapper(ConnectionWrapper.this, statement, connectionResult.getSchemas());
                }
            };
        }

        @Override
        public PreparedStatement prepareStatement(String sql) throws SQLException {
            return new DDRPreparedStatementImpl(sql, isReadOnly0(), getSchemas0()) {

                @Override
                public SQLParsedResult parseSql(String sql, Map<Object, Object> jdbcParams) throws SQLException {
                    return AbstractDDRDataSource.this.parseSql(sql, jdbcParams);
                }

                @Override
                public synchronized StatementWrapper getStatement(DataSourceParam param, String routedSql)
                                                                                                          throws SQLException {
                    ConnectionResult connectionResult = getConnection0(param);
                    Statement statement = connectionResult.getConnection().prepareStatement(routedSql);
                    return new StatementWrapper(ConnectionWrapper.this, statement, connectionResult.getSchemas());
                }
            };
        }

        @Override
        public Statement createStatement(final int resultSetType, final int resultSetConcurrency) throws SQLException {
            return new DDRStatementImpl(isReadOnly0(), getSchemas0()) {

                @Override
                public SQLParsedResult parseSql(String sql, Map<Object, Object> jdbcParams) throws SQLException {
                    return AbstractDDRDataSource.this.parseSql(sql, jdbcParams);
                }

                @Override
                public synchronized StatementWrapper getStatement(DataSourceParam param, String sql)
                                                                                                    throws SQLException {
                    ConnectionResult connectionResult = getConnection0(param);
                    Statement statement = connectionResult.getConnection().createStatement(resultSetType,
                                                                                           resultSetConcurrency);
                    return new StatementWrapper(ConnectionWrapper.this, statement, connectionResult.getSchemas());
                }
            };
        }

        @Override
        public PreparedStatement prepareStatement(String sql, final int resultSetType, final int resultSetConcurrency)
                                                                                                                      throws SQLException {
            return new DDRPreparedStatementImpl(sql, isReadOnly0(), getSchemas0()) {

                @Override
                public SQLParsedResult parseSql(String sql, Map<Object, Object> jdbcParams) throws SQLException {
                    return AbstractDDRDataSource.this.parseSql(sql, jdbcParams);
                }

                @Override
                public synchronized StatementWrapper getStatement(DataSourceParam param, String routedSql)
                                                                                                          throws SQLException {
                    ConnectionResult connectionResult = getConnection0(param);
                    Statement statement = connectionResult.getConnection().prepareStatement(routedSql, resultSetType,
                                                                                            resultSetConcurrency);
                    return new StatementWrapper(ConnectionWrapper.this, statement, connectionResult.getSchemas());
                }
            };
        }

        @Override
        public Statement createStatement(final int resultSetType, final int resultSetConcurrency,
                                         final int resultSetHoldability) throws SQLException {
            return new DDRStatementImpl(isReadOnly0(), getSchemas0()) {

                @Override
                public SQLParsedResult parseSql(String sql, Map<Object, Object> jdbcParams) throws SQLException {
                    return AbstractDDRDataSource.this.parseSql(sql, jdbcParams);
                }

                @Override
                public synchronized StatementWrapper getStatement(DataSourceParam param, String sql)
                                                                                                    throws SQLException {
                    ConnectionResult connectionResult = getConnection0(param);
                    Statement statement = connection.createStatement(resultSetType, resultSetConcurrency,
                                                                     resultSetHoldability);
                    return new StatementWrapper(ConnectionWrapper.this, statement, connectionResult.getSchemas());
                }
            };
        }

        @Override
        public PreparedStatement prepareStatement(String sql, final int resultSetType, final int resultSetConcurrency,
                                                  final int resultSetHoldability) throws SQLException {
            return new DDRPreparedStatementImpl(sql, isReadOnly0(), getSchemas0()) {

                @Override
                public SQLParsedResult parseSql(String sql, Map<Object, Object> jdbcParams) throws SQLException {
                    return AbstractDDRDataSource.this.parseSql(sql, jdbcParams);
                }

                @Override
                public synchronized StatementWrapper getStatement(DataSourceParam param, String routedSql)
                                                                                                          throws SQLException {
                    ConnectionResult connectionResult = getConnection0(param);
                    Statement statement = connection.prepareStatement(routedSql, resultSetType, resultSetConcurrency,
                                                                      resultSetHoldability);
                    return new StatementWrapper(ConnectionWrapper.this, statement, connectionResult.getSchemas());
                }
            };
        }

        @Override
        public PreparedStatement prepareStatement(String sql, final int autoGeneratedKeys) throws SQLException {
            return new DDRPreparedStatementImpl(sql, isReadOnly0(), getSchemas0()) {

                @Override
                public SQLParsedResult parseSql(String sql, Map<Object, Object> jdbcParams) throws SQLException {
                    return AbstractDDRDataSource.this.parseSql(sql, jdbcParams);
                }

                @Override
                public synchronized StatementWrapper getStatement(DataSourceParam param, String routedSql)
                                                                                                          throws SQLException {
                    ConnectionResult connectionResult = getConnection0(param);
                    Statement statement = connection.prepareStatement(routedSql, autoGeneratedKeys);
                    return new StatementWrapper(ConnectionWrapper.this, statement, connectionResult.getSchemas());
                }
            };
        }

        @Override
        public PreparedStatement prepareStatement(String sql, final int[] columnIndexes) throws SQLException {
            return new DDRPreparedStatementImpl(sql, isReadOnly0(), getSchemas0()) {

                @Override
                public SQLParsedResult parseSql(String sql, Map<Object, Object> jdbcParams) throws SQLException {
                    return AbstractDDRDataSource.this.parseSql(sql, jdbcParams);
                }

                @Override
                public synchronized StatementWrapper getStatement(DataSourceParam param, String routedSql)
                                                                                                          throws SQLException {
                    ConnectionResult connectionResult = getConnection0(param);
                    Statement statement = connectionResult.getConnection().prepareStatement(routedSql, columnIndexes);
                    return new StatementWrapper(ConnectionWrapper.this, statement, connectionResult.getSchemas());
                }
            };
        }

        @Override
        public PreparedStatement prepareStatement(String sql, final String[] columnNames) throws SQLException {
            return new DDRPreparedStatementImpl(sql, isReadOnly0(), getSchemas0()) {

                @Override
                public SQLParsedResult parseSql(String sql, Map<Object, Object> jdbcParams) throws SQLException {
                    return AbstractDDRDataSource.this.parseSql(sql, jdbcParams);
                }

                @Override
                public synchronized StatementWrapper getStatement(DataSourceParam param, String routedSql)
                                                                                                          throws SQLException {
                    ConnectionResult connectionResult = getConnection0(param);
                    Statement statement = connectionResult.getConnection().prepareStatement(routedSql, columnNames);
                    return new StatementWrapper(ConnectionWrapper.this, statement, connectionResult.getSchemas());
                }
            };
        }

        // 未初始化前可以调用的方法
        @Override
        public synchronized void setAutoCommit(boolean autoCommit) throws SQLException {
            Connection connection = getConnection1();
            if (connection != null) {
                connection.setAutoCommit(autoCommit);
            }
            tag.setAutoCommit(true);
            prop.setAutoCommit(autoCommit);
        }

        @Override
        public synchronized void setReadOnly(boolean readOnly) throws SQLException {
            Connection connection = getConnection1();
            if (connection != null) {
                connection.setReadOnly(readOnly);
            }
            tag.setReadOnly(true);
            prop.setReadOnly(readOnly);
        }

        @Override
        public synchronized void setCatalog(String catalog) throws SQLException {
            Connection connection = getConnection1();
            if (connection != null) {
                connection.setCatalog(catalog);
            }
            tag.setCatalog(true);
            prop.setCatalog(catalog);
        }

        @Override
        public synchronized void setTransactionIsolation(int level) throws SQLException {
            Connection connection = getConnection1();
            if (connection != null) {
                connection.setTransactionIsolation(level);
            }
            tag.setTransactionIsolation(true);
            prop.setTransactionIsolation(level);
        }

        @Override
        public synchronized SQLWarning getWarnings() throws SQLException {
            Connection connection = getConnection1();
            if (connection != null) {
                return connection.getWarnings();
            } else {
                throw new UninitializedStatusException("Can't invoke 'getWarnings()' before connection is initialized");
            }
        }

        @Override
        public synchronized void clearWarnings() throws SQLException {
            Connection connection = getConnection1();
            if (connection != null) {
                connection.clearWarnings();
            } else {
                throw new UninitializedStatusException(
                                                       "Can't invoke 'clearWarnings()' before connection is initialized");
            }
        }

        @Override
        public synchronized void setTypeMap(Map<String, Class<?>> map) throws SQLException {
            Connection connection = getConnection1();
            if (connection != null) {
                connection.setTypeMap(map);
            }
            tag.setTypeMap(true);
            prop.setTypeMap(map);
        }

        @Override
        public synchronized void setHoldability(int holdability) throws SQLException {
            Connection connection = getConnection1();
            if (connection != null) {
                connection.setHoldability(holdability);
            }
            tag.setHoldability(true);
            prop.setHoldability(holdability);
        }

        @Override
        public synchronized void setSchema(String schema) throws SQLException {
            Connection connection = getConnection1();
            if (connection != null) {
                connection.setSchema(schema);
            }
            tag.setSchema(true);
            prop.setSchema(schema);
        }

        // 初始化后才能调动的方法
        @Override
        public synchronized void setClientInfo(String name, String value) throws SQLClientInfoException {
            Connection connection = getConnection1();
            if (connection != null) {
                connection.setClientInfo(name, value);
            } else {
                throw new UninitializedStatusException(
                                                       "Can't invoke 'setClientInfo(String name, String value)' before connection is initialized");
            }
        }

        @Override
        public synchronized void setClientInfo(Properties properties) throws SQLClientInfoException {
            Connection connection = getConnection1();
            if (connection != null) {
                connection.setClientInfo(properties);
            } else {
                throw new UninitializedStatusException(
                                                       "Can't invoke 'setClientInfo(Properties properties)' before connection is initialized");
            }
        }

        @Override
        public synchronized String getClientInfo(String name) throws SQLException {
            Connection connection = getConnection1();
            if (connection != null) {
                return connection.getClientInfo(name);
            } else {
                throw new UninitializedStatusException(
                                                       "Can't invoke 'getClientInfo(String name)' before connection is initialized");
            }
        }

        @Override
        public synchronized Properties getClientInfo() throws SQLException {
            Connection connection = getConnection1();
            if (connection != null) {
                return connection.getClientInfo();
            } else {
                throw new UninitializedStatusException(
                                                       "Can't invoke 'getClientInfo()' before connection is initialized");
            }
        }

        @Override
        public synchronized void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
            Connection connection = getConnection1();
            if (connection != null) {
                connection.setNetworkTimeout(executor, milliseconds);
            } else {
                throw new UninitializedStatusException(
                                                       "Can't invoke 'setNetworkTimeout(Executor executor, int milliseconds)' before connection is initialized");
            }
        }

        @Override
        public synchronized int getNetworkTimeout() throws SQLException {
            Connection connection = getConnection1();
            if (connection != null) {
                return connection.getNetworkTimeout();
            } else {
                throw new UninitializedStatusException(
                                                       "Can't invoke 'getNetworkTimeout()' before connection is initialized");
            }
        }

        @Override
        public synchronized Savepoint setSavepoint() throws SQLException {
            Connection connection = getConnection1();
            if (connection != null) {
                return connection.setSavepoint();
            } else {
                throw new UninitializedStatusException("Can't invoke 'setSavepoint()' before connection is initialized");
            }
        }

        @Override
        public synchronized Savepoint setSavepoint(String name) throws SQLException {
            Connection connection = getConnection1();
            if (connection != null) {
                return connection.setSavepoint(name);
            } else {
                throw new UninitializedStatusException(
                                                       "Can't invoke 'setSavepoint(name)' before connection is initialized");
            }
        }

        @Override
        public synchronized void rollback(Savepoint savepoint) throws SQLException {
            Connection connection = getConnection1();
            if (connection != null) {
                connection.rollback(savepoint);
            } else {
                throw new UninitializedStatusException(
                                                       "Can't invoke 'rollback(Savepoint savepoint)' before connection is initialized");
            }
        }

        @Override
        public synchronized void releaseSavepoint(Savepoint savepoint) throws SQLException {
            Connection connection = getConnection1();
            if (connection != null) {
                connection.releaseSavepoint(savepoint);
            } else {
                throw new UninitializedStatusException(
                                                       "Can't invoke 'releaseSavepoint(Savepoint savepoint)' before connection is initialized");
            }
        }

        @Override
        public synchronized Clob createClob() throws SQLException {
            Connection connection = getConnection1();
            if (connection != null) {
                return connection.createClob();
            } else {
                throw new UninitializedStatusException("Can't invoke 'createClob()' before connection is initialized");
            }
        }

        @Override
        public synchronized Blob createBlob() throws SQLException {
            Connection connection = getConnection1();
            if (connection != null) {
                return connection.createBlob();
            } else {
                throw new UninitializedStatusException("Can't invoke 'createBlob()' before connection is initialized");
            }
        }

        @Override
        public synchronized NClob createNClob() throws SQLException {
            Connection connection = getConnection1();
            if (connection != null) {
                return connection.createNClob();
            } else {
                throw new UninitializedStatusException("Can't invoke 'createNClob()' before connection is initialized");
            }
        }

        @Override
        public synchronized SQLXML createSQLXML() throws SQLException {
            Connection connection = getConnection1();
            if (connection != null) {
                return connection.createSQLXML();
            } else {
                throw new UninitializedStatusException("Can't invoke 'createSQLXML()' before connection is initialized");
            }
        }

        @Override
        public synchronized boolean isValid(int timeout) throws SQLException {
            Connection connection = getConnection1();
            if (connection != null) {
                return connection.isValid(timeout);
            } else {
                throw new UninitializedStatusException(
                                                       "Can't invoke 'isValid(int timeout)' before connection is initialized");
            }
        }

        @Override
        public synchronized Array createArrayOf(String typeName, Object[] elements) throws SQLException {
            Connection connection = getConnection1();
            if (connection != null) {
                return connection.createArrayOf(typeName, elements);
            } else {
                throw new UninitializedStatusException(
                                                       "Can't invoke 'createArrayOf(String typeName, Object[] elements)' before connection is initialized");
            }
        }

        @Override
        public synchronized Struct createStruct(String typeName, Object[] attributes) throws SQLException {
            Connection connection = getConnection1();
            if (connection != null) {
                return connection.createStruct(typeName, attributes);
            } else {
                throw new UninitializedStatusException(
                                                       "Can't invoke 'createStruct(String typeName, Object[] attributes)' before connection is initialized");
            }
        }

        @Override
        public synchronized void commit() throws SQLException {
            if (connectionResult != null) {
                connectionResult.getConnection().commit();
                closeConnection0(connectionResult.getConnection());
                connectionResult = null;
            } else {
                // ignore
            }
        }

        @Override
        public synchronized void rollback() throws SQLException {
            if (connectionResult != null) {
                connectionResult.getConnection().rollback();
                closeConnection0(connectionResult.getConnection());
                connectionResult = null;
            } else {
                // ignore
            }
        }

        @Override
        public synchronized void close() throws SQLException {
            if (connectionResult != null) {
                connectionResult.getConnection().close();
                connectionResult = null;
            } else {
                // ignore
            }
        }

        @Override
        public synchronized boolean isClosed() throws SQLException {
            Connection connection = getConnection1();
            if (connection != null) {// TODO
                return connection.isClosed();
            } else {
                return false;
            }
        }

        // 需要初始化后才能调用的方法
        @Override
        public synchronized void abort(Executor executor) throws SQLException {
            Connection connection = getConnection1();
            if (connection != null) {
                connection.abort(executor);
            } else {
                throw new UninitializedStatusException(
                                                       "Can't invoke 'abort(Executor executor)' before connection is initialized");
            }
        }

        @Override
        public synchronized String nativeSQL(String sql) throws SQLException {
            Connection connection = getConnection1();
            if (connection != null) {
                return connection.nativeSQL(sql);
            } else {
                throw new UninitializedStatusException(
                                                       "Can't invoke 'nativeSQL(String sql)' before connection is initialized");
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

        @Override
        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                             int resultSetHoldability) throws SQLException {
            throw new UnsupportedConnectionInvocationException("prepareCall");
        }

        @Override
        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
                                                                                                     throws SQLException {
            throw new UnsupportedConnectionInvocationException("prepareCall");
        }

        @Override
        public CallableStatement prepareCall(String sql) throws SQLException {
            throw new UnsupportedConnectionInvocationException("prepareCall");
        }

        @Override
        public synchronized boolean isReadOnly() throws SQLException {
            Connection connection = getConnection1();
            if (connection != null) {
                return connection.isReadOnly();
            } else if (tag.isReadOnly()) {
                return prop.isReadOnly();
            } else {
                if (UninitializedConnectionProcessor.isSetDefaultValue(ConnectionProperty.readOnly)) {
                    boolean val = (boolean) UninitializedConnectionProcessor.getDefaultValue(ConnectionProperty.readOnly);
                    if (UninitializedConnectionProcessor.isSyncDefaultValue(ConnectionProperty.readOnly)) {
                        prop.setReadOnly(val);
                        tag.setReadOnly(true);
                    }
                    return val;
                } else {
                    throw new UninitializedStatusException(
                                                           "Can't invoke 'isReadOnly()' before 'setReadOnly(boolean readOnly)' is invoked");
                }
            }
        }

        @Override
        public synchronized String getCatalog() throws SQLException {
            Connection connection = getConnection1();
            if (connection != null) {
                return connection.getCatalog();
            } else if (tag.isCatalog()) {
                return prop.getCatalog();
            } else {
                if (UninitializedConnectionProcessor.isSetDefaultValue(ConnectionProperty.catalog)) {
                    String val = (String) UninitializedConnectionProcessor.getDefaultValue(ConnectionProperty.catalog);
                    if (UninitializedConnectionProcessor.isSyncDefaultValue(ConnectionProperty.catalog)) {
                        prop.setCatalog(val);
                        tag.setCatalog(true);
                    }
                    return val;
                } else {
                    throw new UninitializedStatusException(
                                                           "Can't invoke 'getCatalog()' before 'setCatalog(String catalog)' is invoked or connection is initialized");
                }
            }
        }

        @Override
        public synchronized Map<String, Class<?>> getTypeMap() throws SQLException {
            Connection connection = getConnection1();
            if (connection != null) {
                return connection.getTypeMap();
            } else if (tag.isTypeMap()) {
                return prop.getTypeMap();
            } else {
                if (UninitializedConnectionProcessor.isSetDefaultValue(ConnectionProperty.typeMap)) {
                    Map<String, Class<?>> val = (Map<String, Class<?>>) UninitializedConnectionProcessor.getDefaultValue(ConnectionProperty.typeMap);
                    if (UninitializedConnectionProcessor.isSyncDefaultValue(ConnectionProperty.typeMap)) {
                        prop.setTypeMap(val);
                        tag.setTypeMap(true);
                    }
                    return val;
                } else {
                    throw new UninitializedStatusException(
                                                           "Can't invoke 'getTypeMap()' before 'setTypeMap(Map<String, Class<?>> map)' is invoked or connection is initialized");
                }
            }
        }

        @Override
        public synchronized int getHoldability() throws SQLException {
            Connection connection = getConnection1();
            if (connection != null) {
                return connection.getHoldability();
            } else if (tag.isHoldability()) {
                return prop.getHoldability();
            } else {
                if (UninitializedConnectionProcessor.isSetDefaultValue(ConnectionProperty.holdability)) {
                    int val = ((Number) UninitializedConnectionProcessor.getDefaultValue(ConnectionProperty.holdability)).intValue();
                    if (UninitializedConnectionProcessor.isSyncDefaultValue(ConnectionProperty.holdability)) {
                        prop.setHoldability(val);
                        tag.setHoldability(true);
                    }
                    return val;
                } else {
                    throw new UninitializedStatusException(
                                                           "Can't invoke 'getHoldability()' before 'setHoldability(int holdability)' is invoked or connection is initialized");
                }
            }
        }

        @Override
        public synchronized DatabaseMetaData getMetaData() throws SQLException {
            Connection connection = getConnection1();
            if (connection != null) {
                return connection.getMetaData();
            } else {
                if (UninitializedConnectionProcessor.isSetDefaultValue(ConnectionProperty.metaData)) {
                    DatabaseMetaData val = (DatabaseMetaData) UninitializedConnectionProcessor.getDefaultValue(ConnectionProperty.metaData);
                    if (UninitializedConnectionProcessor.isSyncDefaultValue(ConnectionProperty.metaData)) {
                        prop.setMetaData(val);
                        tag.setMetaData(true);
                    }
                    return val;
                } else {
                    throw new UninitializedStatusException(
                                                           "Can't invoke 'getMetaData()' before connection is initialized");
                }
            }
        }

        @Override
        public synchronized String getSchema() throws SQLException {
            Connection connection = getConnection1();
            if (connection != null) {
                return connection.getSchema();
            } else if (tag.isSchema()) {
                return prop.getSchema();
            } else {
                if (UninitializedConnectionProcessor.isSetDefaultValue(ConnectionProperty.schema)) {
                    String val = (String) UninitializedConnectionProcessor.getDefaultValue(ConnectionProperty.schema);
                    if (UninitializedConnectionProcessor.isSyncDefaultValue(ConnectionProperty.schema)) {
                        prop.setSchema(val);
                        tag.setSchema(true);
                    }
                    return val;
                } else {
                    throw new UninitializedStatusException(
                                                           "Can't invoke 'getSchema()' before 'setSchema(String schema)' is invoked or connection is initialized");
                }
            }
        }

        @Override
        public synchronized int getTransactionIsolation() throws SQLException {
            Connection connection = getConnection1();
            if (connection != null) {
                return connection.getTransactionIsolation();
            } else if (tag.isTransactionIsolation()) {
                return prop.getTransactionIsolation();
            } else {
                if (UninitializedConnectionProcessor.isSetDefaultValue(ConnectionProperty.transactionIsolation)) {
                    int val = ((Number) UninitializedConnectionProcessor.getDefaultValue(ConnectionProperty.transactionIsolation)).intValue();
                    if (UninitializedConnectionProcessor.isSyncDefaultValue(ConnectionProperty.transactionIsolation)) {
                        prop.setTransactionIsolation(val);
                        tag.setTransactionIsolation(true);
                    }
                    return val;
                } else {
                    throw new UninitializedStatusException(
                                                           "Can't invoke 'getTransactionIsolation()' before 'setTransactionIsolation(int level)' is invoked or connection is initialized");
                }
            }
        }

        @Override
        public synchronized boolean getAutoCommit() throws SQLException {
            Connection connection = getConnection1();
            if (connection != null) {
                return connection.getAutoCommit();
            } else if (tag.isAutoCommit()) {
                return prop.isAutoCommit();
            } else {
                if (UninitializedConnectionProcessor.isSetDefaultValue(ConnectionProperty.autoCommit)) {
                    boolean val = (boolean) UninitializedConnectionProcessor.getDefaultValue(ConnectionProperty.autoCommit);
                    if (UninitializedConnectionProcessor.isSyncDefaultValue(ConnectionProperty.autoCommit)) {
                        prop.setAutoCommit(val);
                        tag.setAutoCommit(true);
                    }
                    return val;
                } else {
                    throw new UninitializedStatusException(
                                                           "Can't invoke 'getAutoCommit()' before connection is initialized");
                }
            }
        }
    }
}
