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

import org.hellojavaer.ddr.core.datasource.manage.DataSourceParam;
import org.hellojavaer.ddr.core.exception.DDRException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">zoukaiming[邹凯明]</a>,created on 05/11/2016.
 */
public abstract class AbstractDefaultDDRDataSource implements DDRDataSource {

    protected final Logger     logger = LoggerFactory.getLogger(getClass());

    private DataSource         dataSource;
    private DataSourceProperty prop   = new DataSourceProperty();
    private InvokedTag         tag    = new InvokedTag();

    private class DataSourceProperty {

        private int         loginTimeout;
        private PrintWriter logWriter;

        public int getLoginTimeout() {
            return loginTimeout;
        }

        public void setLoginTimeout(int loginTimeout) {
            this.loginTimeout = loginTimeout;
        }

        public PrintWriter getLogWriter() {
            return logWriter;
        }

        public void setLogWriter(PrintWriter logWriter) {
            this.logWriter = logWriter;
        }
    }

    private class InvokedTag {

        private boolean loginTimeout;
        private boolean logWriter;

        public boolean isLoginTimeout() {
            return loginTimeout;
        }

        public void setLoginTimeout(boolean loginTimeout) {
            this.loginTimeout = loginTimeout;
        }

        public boolean isLogWriter() {
            return logWriter;
        }

        public void setLogWriter(boolean logWriter) {
            this.logWriter = logWriter;
        }
    }

    private DataSource getDataSource0(DataSourceParam param) throws SQLException {
        if (dataSource == null) {
            dataSource = this.getDataSource(param);
            if (dataSource == null) {
                throw new DDRException("");// TODO
            } else {
                if (tag.isLoginTimeout()) {
                    dataSource.setLoginTimeout(prop.getLoginTimeout());
                }
                if (tag.isLogWriter()) {
                    dataSource.setLogWriter(prop.getLogWriter());
                }
                logger.debug("");
            }
        }
        return dataSource;
    }

    @Override
    public java.util.logging.Logger getParentLogger() {
        return java.util.logging.Logger.getLogger(java.util.logging.Logger.GLOBAL_LOGGER_NAME);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        if (tag.isLoginTimeout()) {
            return prop.getLoginTimeout();
        } else if (dataSource != null) {
            return dataSource.getLoginTimeout();
        } else {
            throw new DDRException(
                                   "Can't invoke 'getLoginTimeout()' before 'setLoginTimeout(int timeout)' is invoked or datasource is initialized");
        }
    }

    @Override
    public void setLoginTimeout(int timeout) throws SQLException {
        tag.setLoginTimeout(true);
        prop.setLoginTimeout(timeout);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        if (tag.isLogWriter()) {
            return prop.getLogWriter();
        } else if (dataSource != null) {
            return dataSource.getLogWriter();
        } else {
            throw new DDRException(
                                   "Can't invoke 'getLogWriter()' before 'setLogWriter(PrintWriter pw)' is invoked or datasource is initialized");
        }
    }

    @Override
    public void setLogWriter(PrintWriter pw) throws SQLException {
        tag.setLogWriter(true);
        prop.setLogWriter(pw);
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
            public Connection getConnection(DataSourceParam param) {
                try {
                    return getDataSource0(param).getConnection();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    @Override
    public Connection getConnection(final String username, final String password) throws SQLException {
        return new ConnectionWrapper() {

            @Override
            public Connection getConnection(DataSourceParam param) {
                try {
                    return getDataSource0(param).getConnection(username, password);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    private abstract class ConnectionWrapper implements Connection {

        private Connection connection;

        private class ConnectionProperty {

            private boolean               readOnly;
            private boolean               autoCommit;
            private String                catalog;
            private int                   transactionIsolation;
            private Map<String, Class<?>> typeMap;
            private int                   holdability;
            private String                schema;

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
        }

        private class InvokedTag {

            private boolean readOnly;
            private boolean autoCommit;
            private boolean catalog;
            private boolean transactionIsolation;
            private boolean typeMap;
            private boolean holdability;
            private boolean schema;

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
        }

        private ConnectionProperty prop = new ConnectionProperty();
        private InvokedTag         tag  = new InvokedTag();

        private boolean isReadOnly0() {
            return prop.isReadOnly();
        }

        public abstract Connection getConnection(DataSourceParam param);

        private Connection getConnection0(DataSourceParam param) throws SQLException {
            if (connection == null) {
                connection = getConnection(param);
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
            }
            return connection;
        }

        @Override
        public Statement createStatement() throws SQLException {
            return new StatementWrapper(connection.createStatement()) {

                @Override
                public String replaceSql(String sql, Map<Integer, Object> jdbcParams) {
                    return AbstractDefaultDDRDataSource.this.replaceSql(sql, jdbcParams);
                }
            };
        }

        @Override
        public PreparedStatement prepareStatement(String sql) throws SQLException {
            return new PreparedStatementWrapper(sql, isReadOnly0()) {

                @Override
                public String replaceSql(String sql, Map<Integer, Object> jdbcParams) {
                    return AbstractDefaultDDRDataSource.this.replaceSql(sql, jdbcParams);
                }

                @Override
                public PreparedStatement getPreparedStatement(DataSourceParam param, String routedSql) {
                    try {
                        return getConnection0(param).prepareStatement(routedSql);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
        }

        @Override
        public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
            return new StatementWrapper(this.connection.createStatement(resultSetType, resultSetConcurrency)) {

                @Override
                public String replaceSql(String sql, Map<Integer, Object> jdbcParams) {
                    return AbstractDefaultDDRDataSource.this.replaceSql(sql, jdbcParams);
                }
            };
        }

        @Override
        public PreparedStatement prepareStatement(String sql, final int resultSetType, final int resultSetConcurrency)
                                                                                                                      throws SQLException {
            return new PreparedStatementWrapper(sql, isReadOnly0()) {

                @Override
                public String replaceSql(String sql, Map<Integer, Object> jdbcParams) {
                    return AbstractDefaultDDRDataSource.this.replaceSql(sql, jdbcParams);
                }

                @Override
                public PreparedStatement getPreparedStatement(DataSourceParam param, String routedSql) {
                    try {
                        return getConnection0(param).prepareStatement(routedSql, resultSetType, resultSetConcurrency);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
        }

        @Override
        public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
                                                                                                               throws SQLException {
            return new StatementWrapper(this.connection.createStatement(resultSetType, resultSetConcurrency,
                                                                        resultSetHoldability)) {

                @Override
                public String replaceSql(String sql, Map<Integer, Object> jdbcParams) {
                    return AbstractDefaultDDRDataSource.this.replaceSql(sql, jdbcParams);
                }
            };
        }

        @Override
        public PreparedStatement prepareStatement(String sql, final int resultSetType, final int resultSetConcurrency,
                                                  final int resultSetHoldability) throws SQLException {
            return new PreparedStatementWrapper(sql, isReadOnly0()) {

                @Override
                public String replaceSql(String sql, Map<Integer, Object> jdbcParams) {
                    return AbstractDefaultDDRDataSource.this.replaceSql(sql, jdbcParams);
                }

                @Override
                public PreparedStatement getPreparedStatement(DataSourceParam param, String routedSql) {
                    try {
                        return getConnection0(param).prepareStatement(routedSql, resultSetType, resultSetConcurrency,
                                                                      resultSetHoldability);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
        }

        @Override
        public PreparedStatement prepareStatement(String sql, final int autoGeneratedKeys) throws SQLException {
            return new PreparedStatementWrapper(sql, isReadOnly0()) {

                @Override
                public String replaceSql(String sql, Map<Integer, Object> jdbcParams) {
                    return AbstractDefaultDDRDataSource.this.replaceSql(sql, jdbcParams);
                }

                @Override
                public PreparedStatement getPreparedStatement(DataSourceParam param, String routedSql) {
                    try {
                        return getConnection0(param).prepareStatement(routedSql, autoGeneratedKeys);
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
                public String replaceSql(String sql, Map<Integer, Object> jdbcParams) {
                    return AbstractDefaultDDRDataSource.this.replaceSql(sql, jdbcParams);
                }

                @Override
                public PreparedStatement getPreparedStatement(DataSourceParam param, String routedSql) {
                    try {
                        return getConnection0(param).prepareStatement(routedSql, columnIndexes);
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
                public String replaceSql(String sql, Map<Integer, Object> jdbcParams) {
                    return AbstractDefaultDDRDataSource.this.replaceSql(sql, jdbcParams);
                }

                @Override
                public PreparedStatement getPreparedStatement(DataSourceParam param, String routedSql) {
                    try {
                        return getConnection0(param).prepareStatement(routedSql, columnNames);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
        }

        // 未初始化前可以调用的方法
        @Override
        public void setAutoCommit(boolean autoCommit) throws SQLException {
            tag.setAutoCommit(true);
            prop.setAutoCommit(autoCommit);
        }

        @Override
        public boolean getAutoCommit() throws SQLException {
            if (tag.isAutoCommit()) {
                return prop.isAutoCommit();
            } else if (connection != null) {
                return connection.getAutoCommit();
            } else {
                throw new DDRException(
                                       "Can't invoke 'getAutoCommit()' before 'setAutoCommit(boolean autoCommit)' is invoked or connection is initialized");
            }
        }

        @Override
        public void setReadOnly(boolean readOnly) throws SQLException {
            tag.setReadOnly(true);
            prop.setReadOnly(readOnly);
        }

        @Override
        public boolean isReadOnly() throws SQLException {
            if (tag.isReadOnly()) {
                return prop.isReadOnly();
            } else {
                throw new DDRException("Can't invoke 'isReadOnly()' before 'setReadOnly' is invoked");
            }
        }

        @Override
        public void setCatalog(String catalog) throws SQLException {
            tag.setCatalog(true);
            prop.setCatalog(catalog);
        }

        @Override
        public String getCatalog() throws SQLException {
            if (tag.isCatalog()) {
                return prop.getCatalog();
            } else if (connection != null) {
                return connection.getCatalog();
            } else {
                throw new DDRException(
                                       "Can't invoke 'getCatalog()' before 'setCatalog(String catalog)' is invoked or connection is initialized");
            }
        }

        @Override
        public void setTransactionIsolation(int level) throws SQLException {
            tag.setTransactionIsolation(true);
            prop.setTransactionIsolation(level);
        }

        @Override
        public int getTransactionIsolation() throws SQLException {
            if (tag.isTransactionIsolation()) {
                return prop.getTransactionIsolation();
            } else if (connection != null) {
                return connection.getTransactionIsolation();
            } else {
                throw new DDRException(
                                       "Can't invoke 'getTransactionIsolation()' before 'setTransactionIsolation(int level)' is invoked or connection is initialized");
            }
        }

        @Override
        public SQLWarning getWarnings() throws SQLException {
            if (connection != null) {
                return connection.getWarnings();
            } else {
                throw new DDRException("Can't invoke 'getWarnings()' before connection is initialized");
            }
        }

        @Override
        public void clearWarnings() throws SQLException {
            if (connection != null) {
                connection.clearWarnings();
            } else {
                throw new DDRException("Can't invoke 'clearWarnings()' before connection is initialized");
            }
        }

        @Override
        public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
            tag.setTypeMap(true);
            prop.setTypeMap(map);
        }

        @Override
        public Map<String, Class<?>> getTypeMap() throws SQLException {
            if (tag.isTypeMap()) {
                return prop.getTypeMap();
            } else if (connection != null) {
                return connection.getTypeMap();
            } else {
                throw new DDRException(
                                       "Can't invoke 'getTypeMap()' before 'setTypeMap(Map<String, Class<?>> map)' is invoked or connection is initialized");
            }
        }

        @Override
        public void setHoldability(int holdability) throws SQLException {
            tag.setHoldability(true);
            prop.setHoldability(holdability);
        }

        @Override
        public int getHoldability() throws SQLException {
            if (tag.isHoldability()) {
                return prop.getHoldability();
            } else if (connection != null) {
                return connection.getHoldability();
            } else {
                throw new DDRException(
                                       "Can't invoke 'getHoldability()' before 'setHoldability(int holdability)' is invoked or connection is initialized");
            }
        }

        @Override
        public void setSchema(String schema) throws SQLException {
            tag.setSchema(true);
            prop.setSchema(schema);
        }

        @Override
        public String getSchema() throws SQLException {
            if (tag.isSchema()) {
                return prop.getSchema();
            } else if (connection != null) {
                return connection.getSchema();
            } else {
                throw new DDRException(
                                       "Can't invoke 'getSchema()' before 'setSchema(String schema)' is invoked or connection is initialized");
            }
        }

        // 初始化后才能调动的方法

        @Override
        public void setClientInfo(String name, String value) throws SQLClientInfoException {
            if (connection != null) {
                connection.setClientInfo(name, value);
            } else {
                throw new DDRException(
                                       "Can't invoke 'setClientInfo(String name, String value)' before connection is initialized");
            }
        }

        @Override
        public void setClientInfo(Properties properties) throws SQLClientInfoException {
            if (connection != null) {
                connection.setClientInfo(properties);
            } else {
                throw new DDRException(
                                       "Can't invoke 'setClientInfo(Properties properties)' before connection is initialized");
            }
        }

        @Override
        public String getClientInfo(String name) throws SQLException {
            if (connection != null) {
                return connection.getClientInfo(name);
            } else {
                throw new DDRException("Can't invoke 'getClientInfo(String name)' before connection is initialized");
            }
        }

        @Override
        public Properties getClientInfo() throws SQLException {
            if (connection != null) {
                return connection.getClientInfo();
            } else {
                throw new DDRException("Can't invoke 'getClientInfo()' before connection is initialized");
            }
        }

        @Override
        public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
            if (connection != null) {
                connection.setNetworkTimeout(executor, milliseconds);
            } else {
                throw new DDRException(
                                       "Can't invoke 'setNetworkTimeout(Executor executor, int milliseconds)' before connection is initialized");
            }

        }

        @Override
        public int getNetworkTimeout() throws SQLException {
            if (connection != null) {
                return connection.getNetworkTimeout();
            } else {
                throw new DDRException("Can't invoke 'getNetworkTimeout()' before connection is initialized");
            }
        }

        @Override
        public DatabaseMetaData getMetaData() throws SQLException {
            if (connection != null) {
                return connection.getMetaData();
            } else {
                throw new DDRException("Can't invoke 'getMetaData()' before connection is initialized");
            }
        }

        @Override
        public Savepoint setSavepoint() throws SQLException {
            if (connection != null) {
                return connection.setSavepoint();
            } else {
                throw new DDRException("Can't invoke 'setSavepoint()' before connection is initialized");
            }
        }

        @Override
        public Savepoint setSavepoint(String name) throws SQLException {
            if (connection != null) {
                return connection.setSavepoint(name);
            } else {
                throw new DDRException("Can't invoke 'setSavepoint(name)' before connection is initialized");
            }
        }

        @Override
        public void rollback(Savepoint savepoint) throws SQLException {
            if (connection != null) {
                connection.rollback(savepoint);
            } else {
                throw new DDRException("Can't invoke 'rollback(Savepoint savepoint)' before connection is initialized");
            }
        }

        @Override
        public void releaseSavepoint(Savepoint savepoint) throws SQLException {
            if (connection != null) {
                connection.releaseSavepoint(savepoint);
            } else {
                throw new DDRException(
                                       "Can't invoke 'releaseSavepoint(Savepoint savepoint)' before connection is initialized");
            }
        }

        @Override
        public Clob createClob() throws SQLException {
            if (connection != null) {
                return connection.createClob();
            } else {
                throw new DDRException("Can't invoke 'createClob()' before connection is initialized");
            }
        }

        @Override
        public Blob createBlob() throws SQLException {
            if (connection != null) {
                return connection.createBlob();
            } else {
                throw new DDRException("Can't invoke 'createBlob()' before connection is initialized");
            }
        }

        @Override
        public NClob createNClob() throws SQLException {
            if (connection != null) {
                return connection.createNClob();
            } else {
                throw new DDRException("Can't invoke 'createNClob()' before connection is initialized");
            }
        }

        @Override
        public SQLXML createSQLXML() throws SQLException {
            if (connection != null) {
                return connection.createSQLXML();
            } else {
                throw new DDRException("Can't invoke 'createSQLXML()' before connection is initialized");
            }
        }

        @Override
        public boolean isValid(int timeout) throws SQLException {
            if (connection != null) {
                return connection.isValid(timeout);
            } else {
                throw new DDRException("Can't invoke 'isValid(int timeout)' before connection is initialized");
            }
        }

        @Override
        public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
            if (connection != null) {
                return connection.createArrayOf(typeName, elements);
            } else {
                throw new DDRException(
                                       "Can't invoke 'createArrayOf(String typeName, Object[] elements)' before connection is initialized");
            }
        }

        @Override
        public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
            if (connection != null) {
                return connection.createStruct(typeName, attributes);
            } else {
                throw new DDRException(
                                       "Can't invoke 'createStruct(String typeName, Object[] attributes)' before connection is initialized");
            }
        }

        // // 弹性处理的方法
        @Override
        public void commit() throws SQLException {
            if (connection != null) {
                connection.commit();// TODO
            }
        }

        @Override
        public void rollback() throws SQLException {
            if (connection != null) {
                connection.rollback();// TODO
            }
        }

        @Override
        public void close() throws SQLException {
            if (this.connection != null) {
                connection.close();// TODO
            }
        }

        @Override
        public boolean isClosed() throws SQLException {
            if (this.connection != null) {// TODO
                return connection.isClosed();
            } else {
                return false;
            }
        }

        // 需要初始化后才能调用的方法
        @Override
        public void abort(Executor executor) throws SQLException {
            if (connection != null) {
                connection.abort(executor);
            } else {

            }
        }

        @Override
        public String nativeSQL(String sql) throws SQLException {
            if (connection != null) {
                return connection.nativeSQL(sql);
            } else {
                throw new DDRException("Before connection is initialized, 'nativeSQL' can't be invoked");
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
            throw new UnsupportedOperationException("prepareCall");
        }

        @Override
        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
                                                                                                     throws SQLException {
            throw new UnsupportedOperationException("prepareCall");
        }

        @Override
        public CallableStatement prepareCall(String sql) throws SQLException {
            throw new UnsupportedOperationException("prepareCall");
        }
    }

}
