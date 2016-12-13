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
import org.hellojavaer.ddr.core.datasource.jdbc.init.UninitializedConnectionProcessor;
import org.hellojavaer.ddr.core.datasource.jdbc.init.UninitializedDataSourceProcessor;
import org.hellojavaer.ddr.core.datasource.jdbc.property.ConnectionProperty;
import org.hellojavaer.ddr.core.datasource.jdbc.property.DataSourceProperty;
import org.hellojavaer.ddr.core.datasource.manager.DataSourceParam;
import org.hellojavaer.ddr.core.exception.DDRException;
import org.hellojavaer.ddr.core.exception.UninitializedStatusException;
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
 * @author <a href="mailto:hellojavaer@gmail.com">zoukaiming[邹凯明]</a>,created on 05/11/2016.
 */
public abstract class AbstractDefaultDDRDataSource implements DDRDataSource {

    protected final Logger           logger = LoggerFactory.getLogger(getClass());

    private DataSourceSchemasBinding dataSourceSchemasBinding;
    private InnerDataSourceProperty  prop   = new InnerDataSourceProperty();
    private InvocationTag            tag    = new InvocationTag();

    @Override
    public boolean isCrossDataSource(Set<String> schemas) {
        if (dataSourceSchemasBinding == null) {
            return false;
        } else {
            return !dataSourceSchemasBinding.getSchemas().containsAll(schemas);
        }
    }

    private Set<String> getSchemas0() {
        if (dataSourceSchemasBinding == null) {
            return null;
        } else {
            return dataSourceSchemasBinding.getSchemas();
        }
    }

    private boolean isCrossDataSource0(Set<String> schemas) {
        return isCrossDataSource(schemas);
    }

    private class InnerDataSourceProperty {

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

    private class InvocationTag {

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
        if (dataSourceSchemasBinding == null) {
            dataSourceSchemasBinding = this.getDataSource(param);
            if (dataSourceSchemasBinding == null) {
                StringBuilder scNames = new StringBuilder("");
                for (String item : param.getScNames()) {
                    scNames.append(item);
                    scNames.append(',');
                }
                if (scNames.length() > 0) {
                    scNames.deleteCharAt(scNames.length() - 1);
                }
                throw new DDRException("No datasource binding for parameter[readOnly:" + param.isReadOnly()
                                       + ",scNames:'" + scNames.toString() + "']");
            } else {
                if (tag.isLoginTimeout()) {
                    dataSourceSchemasBinding.getDataSource().setLoginTimeout(prop.getLoginTimeout());
                }
                if (tag.isLogWriter()) {
                    dataSourceSchemasBinding.getDataSource().setLogWriter(prop.getLogWriter());
                }
                logger.debug("");
            }
        }
        return dataSourceSchemasBinding.getDataSource();
    }

    @Override
    public java.util.logging.Logger getParentLogger() {
        return java.util.logging.Logger.getLogger(java.util.logging.Logger.GLOBAL_LOGGER_NAME);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        if (tag.isLoginTimeout()) {
            return prop.getLoginTimeout();
        } else if (dataSourceSchemasBinding != null) {
            return dataSourceSchemasBinding.getDataSource().getLoginTimeout();
        } else {
            if (UninitializedDataSourceProcessor.isSetDefaultValue(DataSourceProperty.loginTimeout)) {
                int val = ((Number) UninitializedDataSourceProcessor.getDefaultValue(DataSourceProperty.loginTimeout)).intValue();
                if (UninitializedDataSourceProcessor.isSyncDefaultValue(DataSourceProperty.loginTimeout)) {
                    prop.setLoginTimeout(val);
                    tag.setLoginTimeout(true);
                }
                return val;
            } else {
                throw new UninitializedStatusException(
                                       "Can't invoke 'getLoginTimeout()' before 'setLoginTimeout(int timeout)' is invoked or datasource is initialized");
            }
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
        } else if (dataSourceSchemasBinding != null) {
            return dataSourceSchemasBinding.getDataSource().getLogWriter();
        } else {
            if (UninitializedDataSourceProcessor.isSetDefaultValue(DataSourceProperty.logWriter)) {
                PrintWriter val = (PrintWriter) UninitializedDataSourceProcessor.getDefaultValue(DataSourceProperty.logWriter);
                if (UninitializedDataSourceProcessor.isSyncDefaultValue(DataSourceProperty.logWriter)) {
                    prop.setLogWriter(val);
                    tag.setLogWriter(true);
                }
                return val;
            } else {
                throw new UninitializedStatusException(
                                       "Can't invoke 'getLogWriter()' before 'setLogWriter(PrintWriter pw)' is invoked or datasource is initialized");
            }
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
            public Connection getConnection(DataSourceParam param) throws SQLException {
                return getDataSource0(param).getConnection();
            }
        };
    }

    @Override
    public Connection getConnection(final String username, final String password) throws SQLException {
        return new ConnectionWrapper() {

            @Override
            public Connection getConnection(DataSourceParam param) throws SQLException {
                return getDataSource0(param).getConnection(username, password);
            }
        };
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

        private Connection             connection;
        private ConnectionPropertyBean prop = new ConnectionPropertyBean();
        private InvocationTag          tag  = new InvocationTag();

        private boolean isReadOnly0() {
            return prop.isReadOnly();
        }

        public abstract Connection getConnection(DataSourceParam param) throws SQLException;

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
            return new StatementWrapper(isReadOnly0(), getSchemas0()) {

                @Override
                public DDRDataSource.ReplacedResult replaceSql(String sql, Map<Integer, Object> jdbcParams)
                                                                                                           throws SQLException {
                    return AbstractDefaultDDRDataSource.this.replaceSql(sql, jdbcParams);
                }

                @Override
                public boolean isCrossDataSource(Set<String> schemas) {
                    return isCrossDataSource0(schemas);
                }

                @Override
                public StatementBean getStatement(DataSourceParam param, String sql) throws SQLException {
                    Connection connection = getConnection0(param);
                    Statement statement = connection.createStatement();
                    return new StatementBean(ConnectionWrapper.this, statement, getSchemas0());
                }
            };
        }

        @Override
        public PreparedStatement prepareStatement(String sql) throws SQLException {
            return new PreparedStatementWrapper(sql, isReadOnly0(), getSchemas0()) {

                @Override
                public DDRDataSource.ReplacedResult replaceSql(String sql, Map<Integer, Object> jdbcParams)
                                                                                                           throws SQLException {
                    return AbstractDefaultDDRDataSource.this.replaceSql(sql, jdbcParams);
                }

                @Override
                public boolean isCrossDataSource(Set<String> schemas) {
                    return isCrossDataSource0(schemas);
                }

                @Override
                public StatementBean getStatement(DataSourceParam param, String routedSql) throws SQLException {
                    Connection connection = getConnection0(param);
                    Statement statement = connection.prepareStatement(routedSql);
                    return new StatementBean(ConnectionWrapper.this, statement, getSchemas0());
                }
            };
        }

        @Override
        public Statement createStatement(final int resultSetType, final int resultSetConcurrency) throws SQLException {
            return new StatementWrapper(isReadOnly0(), getSchemas0()) {

                @Override
                public DDRDataSource.ReplacedResult replaceSql(String sql, Map<Integer, Object> jdbcParams)
                                                                                                           throws SQLException {
                    return AbstractDefaultDDRDataSource.this.replaceSql(sql, jdbcParams);
                }

                @Override
                public boolean isCrossDataSource(Set<String> schemas) {
                    return isCrossDataSource0(schemas);
                }

                @Override
                public StatementBean getStatement(DataSourceParam param, String sql) throws SQLException {
                    Connection connection = getConnection0(param);
                    Statement statement = connection.createStatement(resultSetType, resultSetConcurrency);
                    return new StatementBean(ConnectionWrapper.this, statement, getSchemas0());
                }
            };
        }

        @Override
        public PreparedStatement prepareStatement(String sql, final int resultSetType, final int resultSetConcurrency)
                                                                                                                      throws SQLException {
            return new PreparedStatementWrapper(sql, isReadOnly0(), getSchemas0()) {

                @Override
                public DDRDataSource.ReplacedResult replaceSql(String sql, Map<Integer, Object> jdbcParams)
                                                                                                           throws SQLException {
                    return AbstractDefaultDDRDataSource.this.replaceSql(sql, jdbcParams);
                }

                @Override
                public boolean isCrossDataSource(Set<String> schemas) {
                    return isCrossDataSource0(schemas);
                }

                @Override
                public StatementBean getStatement(DataSourceParam param, String routedSql) throws SQLException {
                    Connection connection = getConnection0(param);
                    Statement statement = connection.prepareStatement(routedSql, resultSetType, resultSetConcurrency);
                    return new StatementBean(ConnectionWrapper.this, statement, getSchemas0());
                }
            };
        }

        @Override
        public Statement createStatement(final int resultSetType, final int resultSetConcurrency,
                                         final int resultSetHoldability) throws SQLException {
            return new StatementWrapper(isReadOnly0(), getSchemas0()) {

                @Override
                public DDRDataSource.ReplacedResult replaceSql(String sql, Map<Integer, Object> jdbcParams)
                                                                                                           throws SQLException {
                    return AbstractDefaultDDRDataSource.this.replaceSql(sql, jdbcParams);
                }

                @Override
                public boolean isCrossDataSource(Set<String> schemas) {
                    return isCrossDataSource0(schemas);
                }

                @Override
                public StatementBean getStatement(DataSourceParam param, String sql) throws SQLException {
                    Connection connection = getConnection0(param);
                    Statement statement = connection.createStatement(resultSetType, resultSetConcurrency,
                                                                     resultSetHoldability);
                    return new StatementBean(ConnectionWrapper.this, statement, getSchemas0());
                }
            };
        }

        @Override
        public PreparedStatement prepareStatement(String sql, final int resultSetType, final int resultSetConcurrency,
                                                  final int resultSetHoldability) throws SQLException {
            return new PreparedStatementWrapper(sql, isReadOnly0(), getSchemas0()) {

                @Override
                public DDRDataSource.ReplacedResult replaceSql(String sql, Map<Integer, Object> jdbcParams)
                                                                                                           throws SQLException {
                    return AbstractDefaultDDRDataSource.this.replaceSql(sql, jdbcParams);
                }

                @Override
                public boolean isCrossDataSource(Set<String> schemas) {
                    return isCrossDataSource0(schemas);
                }

                @Override
                public StatementBean getStatement(DataSourceParam param, String routedSql) throws SQLException {
                    Connection connection = getConnection0(param);
                    Statement statement = connection.prepareStatement(routedSql, resultSetType, resultSetConcurrency,
                                                                      resultSetHoldability);
                    return new StatementBean(ConnectionWrapper.this, statement, getSchemas0());
                }
            };
        }

        @Override
        public PreparedStatement prepareStatement(String sql, final int autoGeneratedKeys) throws SQLException {
            return new PreparedStatementWrapper(sql, isReadOnly0(), getSchemas0()) {

                @Override
                public DDRDataSource.ReplacedResult replaceSql(String sql, Map<Integer, Object> jdbcParams)
                                                                                                           throws SQLException {
                    return AbstractDefaultDDRDataSource.this.replaceSql(sql, jdbcParams);
                }

                @Override
                public boolean isCrossDataSource(Set<String> schemas) {
                    return isCrossDataSource0(schemas);
                }

                @Override
                public StatementBean getStatement(DataSourceParam param, String routedSql) throws SQLException {
                    Connection connection = getConnection0(param);
                    Statement statement = connection.prepareStatement(routedSql, autoGeneratedKeys);
                    return new StatementBean(ConnectionWrapper.this, statement, getSchemas0());
                }
            };
        }

        @Override
        public PreparedStatement prepareStatement(String sql, final int[] columnIndexes) throws SQLException {
            return new PreparedStatementWrapper(sql, isReadOnly0(), getSchemas0()) {

                @Override
                public DDRDataSource.ReplacedResult replaceSql(String sql, Map<Integer, Object> jdbcParams)
                                                                                                           throws SQLException {
                    return AbstractDefaultDDRDataSource.this.replaceSql(sql, jdbcParams);
                }

                @Override
                public boolean isCrossDataSource(Set<String> schemas) {
                    return isCrossDataSource0(schemas);
                }

                @Override
                public StatementBean getStatement(DataSourceParam param, String routedSql) throws SQLException {
                    Connection connection = getConnection0(param);
                    Statement statement = connection.prepareStatement(routedSql, columnIndexes);
                    return new StatementBean(ConnectionWrapper.this, statement, getSchemas0());
                }
            };
        }

        @Override
        public PreparedStatement prepareStatement(String sql, final String[] columnNames) throws SQLException {
            return new PreparedStatementWrapper(sql, isReadOnly0(), getSchemas0()) {

                @Override
                public DDRDataSource.ReplacedResult replaceSql(String sql, Map<Integer, Object> jdbcParams)
                                                                                                           throws SQLException {
                    return AbstractDefaultDDRDataSource.this.replaceSql(sql, jdbcParams);
                }

                @Override
                public boolean isCrossDataSource(Set<String> schemas) {
                    return isCrossDataSource0(schemas);
                }

                @Override
                public StatementBean getStatement(DataSourceParam param, String routedSql) throws SQLException {
                    Connection connection = getConnection0(param);
                    Statement statement = connection.prepareStatement(routedSql, columnNames);
                    return new StatementBean(ConnectionWrapper.this, statement, getSchemas0());
                }
            };
        }

        // 未初始化前可以调用的方法
        @Override
        public void setAutoCommit(boolean autoCommit) throws SQLException {
            if (connection != null) {
                connection.setAutoCommit(autoCommit);
            } else {
                tag.setAutoCommit(true);
                prop.setAutoCommit(autoCommit);
            }
        }

        @Override
        public void setReadOnly(boolean readOnly) throws SQLException {
            if (connection != null) {
                connection.setReadOnly(readOnly);
            } else {
                tag.setReadOnly(true);
                prop.setReadOnly(readOnly);
            }
        }

        @Override
        public void setCatalog(String catalog) throws SQLException {
            if (connection != null) {
                connection.setCatalog(catalog);
            } else {
                tag.setCatalog(true);
                prop.setCatalog(catalog);
            }
        }

        @Override
        public void setTransactionIsolation(int level) throws SQLException {
            if (connection != null) {
                connection.setTransactionIsolation(level);
            } else {
                tag.setTransactionIsolation(true);
                prop.setTransactionIsolation(level);
            }
        }

        @Override
        public SQLWarning getWarnings() throws SQLException {
            if (connection != null) {
                return connection.getWarnings();
            } else {
                throw new UninitializedStatusException("Can't invoke 'getWarnings()' before connection is initialized");
            }
        }

        @Override
        public void clearWarnings() throws SQLException {
            if (connection != null) {
                connection.clearWarnings();
            } else {
                throw new UninitializedStatusException("Can't invoke 'clearWarnings()' before connection is initialized");
            }
        }

        @Override
        public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
            if (connection != null) {
                connection.setTypeMap(map);
            } else {
                tag.setTypeMap(true);
                prop.setTypeMap(map);
            }
        }

        @Override
        public void setHoldability(int holdability) throws SQLException {
            if (connection != null) {
                connection.setHoldability(holdability);
            } else {
                tag.setHoldability(true);
                prop.setHoldability(holdability);
            }
        }

        @Override
        public void setSchema(String schema) throws SQLException {
            if (connection != null) {
                connection.setSchema(schema);
            } else {
                tag.setSchema(true);
                prop.setSchema(schema);
            }
        }

        // 初始化后才能调动的方法
        @Override
        public void setClientInfo(String name, String value) throws SQLClientInfoException {
            if (connection != null) {
                connection.setClientInfo(name, value);
            } else {
                throw new UninitializedStatusException(
                                       "Can't invoke 'setClientInfo(String name, String value)' before connection is initialized");
            }
        }

        @Override
        public void setClientInfo(Properties properties) throws SQLClientInfoException {
            if (connection != null) {
                connection.setClientInfo(properties);
            } else {
                throw new UninitializedStatusException(
                                       "Can't invoke 'setClientInfo(Properties properties)' before connection is initialized");
            }
        }

        @Override
        public String getClientInfo(String name) throws SQLException {
            if (connection != null) {
                return connection.getClientInfo(name);
            } else {
                throw new UninitializedStatusException("Can't invoke 'getClientInfo(String name)' before connection is initialized");
            }
        }

        @Override
        public Properties getClientInfo() throws SQLException {
            if (connection != null) {
                return connection.getClientInfo();
            } else {
                throw new UninitializedStatusException("Can't invoke 'getClientInfo()' before connection is initialized");
            }
        }

        @Override
        public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
            if (connection != null) {
                connection.setNetworkTimeout(executor, milliseconds);
            } else {
                throw new UninitializedStatusException(
                                       "Can't invoke 'setNetworkTimeout(Executor executor, int milliseconds)' before connection is initialized");
            }
        }

        @Override
        public int getNetworkTimeout() throws SQLException {
            if (connection != null) {
                return connection.getNetworkTimeout();
            } else {
                throw new UninitializedStatusException("Can't invoke 'getNetworkTimeout()' before connection is initialized");
            }
        }

        @Override
        public Savepoint setSavepoint() throws SQLException {
            if (connection != null) {
                return connection.setSavepoint();
            } else {
                throw new UninitializedStatusException("Can't invoke 'setSavepoint()' before connection is initialized");
            }
        }

        @Override
        public Savepoint setSavepoint(String name) throws SQLException {
            if (connection != null) {
                return connection.setSavepoint(name);
            } else {
                throw new UninitializedStatusException("Can't invoke 'setSavepoint(name)' before connection is initialized");
            }
        }

        @Override
        public void rollback(Savepoint savepoint) throws SQLException {
            if (connection != null) {
                connection.rollback(savepoint);
            } else {
                throw new UninitializedStatusException("Can't invoke 'rollback(Savepoint savepoint)' before connection is initialized");
            }
        }

        @Override
        public void releaseSavepoint(Savepoint savepoint) throws SQLException {
            if (connection != null) {
                connection.releaseSavepoint(savepoint);
            } else {
                throw new UninitializedStatusException(
                                       "Can't invoke 'releaseSavepoint(Savepoint savepoint)' before connection is initialized");
            }
        }

        @Override
        public Clob createClob() throws SQLException {
            if (connection != null) {
                return connection.createClob();
            } else {
                throw new UninitializedStatusException("Can't invoke 'createClob()' before connection is initialized");
            }
        }

        @Override
        public Blob createBlob() throws SQLException {
            if (connection != null) {
                return connection.createBlob();
            } else {
                throw new UninitializedStatusException("Can't invoke 'createBlob()' before connection is initialized");
            }
        }

        @Override
        public NClob createNClob() throws SQLException {
            if (connection != null) {
                return connection.createNClob();
            } else {
                throw new UninitializedStatusException("Can't invoke 'createNClob()' before connection is initialized");
            }
        }

        @Override
        public SQLXML createSQLXML() throws SQLException {
            if (connection != null) {
                return connection.createSQLXML();
            } else {
                throw new UninitializedStatusException("Can't invoke 'createSQLXML()' before connection is initialized");
            }
        }

        @Override
        public boolean isValid(int timeout) throws SQLException {
            if (connection != null) {
                return connection.isValid(timeout);
            } else {
                throw new UninitializedStatusException("Can't invoke 'isValid(int timeout)' before connection is initialized");
            }
        }

        @Override
        public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
            if (connection != null) {
                return connection.createArrayOf(typeName, elements);
            } else {
                throw new UninitializedStatusException(
                                       "Can't invoke 'createArrayOf(String typeName, Object[] elements)' before connection is initialized");
            }
        }

        @Override
        public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
            if (connection != null) {
                return connection.createStruct(typeName, attributes);
            } else {
                throw new UninitializedStatusException(
                                       "Can't invoke 'createStruct(String typeName, Object[] attributes)' before connection is initialized");
            }
        }

        @Override
        public void commit() throws SQLException {
            if (connection != null) {
                connection.commit();// TODO
            } else {
                throw new UninitializedStatusException("Can't invoke 'commit()' before connection is initialized");
            }
        }

        @Override
        public void rollback() throws SQLException {
            if (connection != null) {
                connection.rollback();
            } else {
                // ignore
            }
        }

        // 弹性处理的方法
        @Override
        public void close() throws SQLException {
            if (connection != null) {
                connection.close();// TODO
            } else {
                // ignore
            }
        }

        @Override
        public boolean isClosed() throws SQLException {
            if (connection != null) {// TODO
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
                throw new UninitializedStatusException("Can't invoke 'abort(Executor executor)' before connection is initialized");
            }
        }

        @Override
        public String nativeSQL(String sql) throws SQLException {
            if (connection != null) {
                return connection.nativeSQL(sql);
            } else {
                throw new UninitializedStatusException("Can't invoke 'nativeSQL(String sql)' before connection is initialized");
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

        @Override
        public boolean isReadOnly() throws SQLException {
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
        public String getCatalog() throws SQLException {
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
        public Map<String, Class<?>> getTypeMap() throws SQLException {
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
        public int getHoldability() throws SQLException {
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
        public DatabaseMetaData getMetaData() throws SQLException {
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
                    throw new UninitializedStatusException("Can't invoke 'getMetaData()' before connection is initialized");
                }
            }
        }

        @Override
        public String getSchema() throws SQLException {
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
        public int getTransactionIsolation() throws SQLException {
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
        public boolean getAutoCommit() throws SQLException {
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
                    throw new UninitializedStatusException("Can't invoke 'getAutoCommit()' before connection is initialized");
                }
            }
        }
    }
}
