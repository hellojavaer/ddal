/*
 * Copyright 2017-2017 the original author or authors.
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
package org.hellojavaer.ddal.sequence.db;

import org.hellojavaer.ddal.sequence.IdGetter;
import org.hellojavaer.ddal.sequence.IdRange;
import org.hellojavaer.ddal.sequence.utils.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ConcurrentModificationException;

/**
 *
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 04/01/2017.
 */
public class DatabaseIdGetter implements IdGetter {

    private Logger          logger                = LoggerFactory.getLogger(this.getClass());

    /*** SELECT id, current_value, end_value, version FROM sc_name.sequence WHERE group_name = ? AND table_name = ? AND deleted = 0 ODER BY select_oder ASC LIMIT 1 ***/
    private String          selectSqlTemplate     = "SELECT %s, %s, %s, %s FROM %s.%s WHERE %s = ? AND %s = ? AND %s = 0 ORDER BY %s ASC LIMIT 1 ";

    /*** UPDATE sc_name.sequence SET current_value = ?, deleted = ?, version = version + 1 WHERE id = ? AND version = ? LIMIT 1 ***/
    private String          updateSqlTemplate     = "UPDATE %s.%s SET %s = ?, %s = ?, %s = %s + 1 WHERE %s = ? AND %s = ? LIMIT 1";

    private DataSource      dataSource;
    private Connection      connection;
    private String          scName;
    private String          tbName                = "sequence";
    private Integer         skipNSteps            = 0;

    private String          colNameOfPrimaryKey   = "id";
    private String          colNameOfGroupName    = "group_name";
    private String          colNameOfTableName    = "table_name";
    private String          colNameOfSelectOrder  = "select_order";
    private String          colNameOfEndValue     = "end_value";
    private String          colNameOfCurrentValue = "current_value";
    private String          colNameOfDeleted      = "deleted";
    private String          colNameOfVersion      = "version";

    private volatile String targetSelectSql;
    private volatile String targetUpdateSql;
    private boolean         initialized           = false;

    public DatabaseIdGetter() {
    }

    public DatabaseIdGetter(DataSource dataSource, String scName) {
        this.dataSource = dataSource;
        this.scName = scName;
    }

    public DatabaseIdGetter(DataSource dataSource, String scName, String tbName) {
        this.dataSource = dataSource;
        this.scName = scName;
        this.tbName = tbName;
    }

    public DatabaseIdGetter(Connection connection, String scName) {
        this.connection = connection;
        this.scName = scName;
    }

    public DatabaseIdGetter(Connection connection, String scName, String tbName) {
        this.connection = connection;
        this.scName = scName;
        this.tbName = tbName;
    }

    protected String getSelectSqlTemplate() {
        return selectSqlTemplate;
    }

    protected String getUpdateSqlTemplate() {
        return updateSqlTemplate;
    }

    public void init() {
        if (!initialized) {
            synchronized (this) {
                if (!initialized) {
                    Assert.isTrue(dataSource != null && connection == null || dataSource == null && connection != null,
                                  "'dataSource' and 'connection', only one of them should be configured");
                    Assert.notNull(scName, "'scName' can't be null");
                    Assert.notNull(tbName, "'tbName' can't be null");
                    Assert.notNull(colNameOfPrimaryKey, "'colNameOfPrimaryKey' can't be null");
                    Assert.notNull(colNameOfGroupName, "'colNameOfGroupName' can't be null");
                    Assert.notNull(colNameOfTableName, "'colNameOfTableName' can't be null");
                    Assert.notNull(colNameOfEndValue, "'colNameOfEndValue' can't be null");
                    Assert.notNull(colNameOfCurrentValue, "'colNameOfCurrentValue' can't be null");
                    Assert.notNull(colNameOfSelectOrder, "'colNameOfSelectOrder' can't be null");
                    Assert.notNull(colNameOfDeleted, "'colNameOfDeleted' can't be null");
                    Assert.notNull(colNameOfVersion, "'colNameOfVersion' can't be null");
                    Assert.isTrue(skipNSteps != null && skipNSteps >= 0, "'skipNSteps'[" + skipNSteps
                                                                         + "] must greater than or equal to 0");

                    /*** SELECT id, current_value, end_value, version FROM sc_name.sequence WHERE group_name = ? AND table_name = ? AND deleted = 0 ORDER BY select_oder ASC LIMIT 1 ***/
                    // "SELECT %s, %s, %s, %s FROM %s.%s WHERE %s = ? AND %s = ? AND %s = 0 ODER BY %s ASC LIMIT 1 ";
                    targetSelectSql = String.format(getSelectSqlTemplate(), colNameOfPrimaryKey, colNameOfCurrentValue,
                                                    colNameOfEndValue,
                                                    colNameOfVersion,//
                                                    scName,
                                                    tbName,//
                                                    colNameOfGroupName, colNameOfTableName, colNameOfDeleted,
                                                    colNameOfSelectOrder);

                    /*** UPDATE sc_name.sequence SET current_value = ?, deleted = ?, version = version + 1 WHERE id = ? AND version = ? LIMIT 1 ***/
                    // "UPDATE %s.%s SET %s = ?, %s = ?, %s = %s + 1 WHERE %s = ? AND %s = ? LIMIT 1";
                    targetUpdateSql = String.format(getUpdateSqlTemplate(), scName, tbName, colNameOfCurrentValue,
                                                    colNameOfDeleted, colNameOfVersion, colNameOfVersion,
                                                    colNameOfPrimaryKey, colNameOfVersion);
                    initialized = true;
                }
            }
        }
    }

    @Override
    public IdRange get(String groupName, String tableName, int step) throws Exception {
        init();
        int rows = 0;
        IdRange idRange = null;
        Connection connection = this.connection;
        PreparedStatement selectStatement = null;
        PreparedStatement updateStatement = null;
        boolean isNewConnection = false;
        if (connection == null) {
            connection = dataSource.getConnection();
            isNewConnection = true;
            connection.setAutoCommit(false);
        }
        try {
            selectStatement = connection.prepareStatement(targetSelectSql);
            selectStatement.setString(1, groupName);
            selectStatement.setString(2, tableName);
            ResultSet selectResult = selectStatement.executeQuery();
            long id = 0;
            long currentValue = 0;
            Long endValue = null;
            int deleted = 0;
            long version = 0;
            if (selectResult.next()) {
                id = ((Number) selectResult.getObject(1)).longValue();
                currentValue = ((Number) selectResult.getLong(2)).longValue();
                Object endValueObj = selectResult.getObject(3);
                if (endValueObj != null) {
                    endValue = ((Number) endValueObj).longValue();
                }
                version = ((Number) selectResult.getLong(4)).longValue();
            } else {
                return null;
            }
            updateStatement = connection.prepareStatement(targetUpdateSql);
            long oneStepEndId = currentValue + step;
            long nStepEndId = currentValue + step * (skipNSteps + 1);
            if (endValue != null && nStepEndId >= endValue) {
                if (nStepEndId > endValue) {
                    if (logger.isWarnEnabled()) {
                        logger.warn(String.format("'nStepEndId'[%s] is more than 'endValue'[%s] limit, and 'nStepEndId' is reset to 'endValue'. " //
                                                          + "More detail information is groupName:%s,tabName:%s,step:%s,currentValue:%s,version:%s,id:%s}",//
                                                  nStepEndId, endValue,//
                                                  groupName, tableName, step, currentValue, version, id));
                        nStepEndId = endValue;
                    }
                }
                deleted = 1;
            }
            updateStatement.setLong(1, nStepEndId);
            updateStatement.setLong(2, deleted);
            updateStatement.setLong(3, id);
            updateStatement.setLong(4, version);
            rows = updateStatement.executeUpdate();
            if (isNewConnection) {
                connection.commit();
            }
            if (rows > 0) {
                idRange = new IdRange(currentValue + 1, oneStepEndId);
            }
        } finally {
            if (selectStatement != null) {
                try {
                    selectStatement.close();
                } catch (Throwable e) {
                    // ignore
                }
            }
            if (updateStatement != null) {
                try {
                    updateStatement.close();
                } catch (Throwable e) {
                    // ignore
                }
            }
            if (isNewConnection && connection != null) {
                try {
                    connection.close();
                } catch (Throwable e) {
                    // ignore
                }
            }
        }
        if (rows == 0) {
            throw new ConcurrentModificationException();
        } else {
            return idRange;
        }
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public String getScName() {
        return scName;
    }

    public void setScName(String scName) {
        this.scName = scName;
    }

    public String getTbName() {
        return tbName;
    }

    public void setTbName(String tbName) {
        this.tbName = tbName;
    }

    public Integer getSkipNSteps() {
        return skipNSteps;
    }

    public void setSkipNSteps(Integer skipNSteps) {
        this.skipNSteps = skipNSteps;
    }

    public String getColNameOfPrimaryKey() {
        return colNameOfPrimaryKey;
    }

    public void setColNameOfPrimaryKey(String colNameOfPrimaryKey) {
        this.colNameOfPrimaryKey = colNameOfPrimaryKey;
    }

    public String getColNameOfGroupName() {
        return colNameOfGroupName;
    }

    public void setColNameOfGroupName(String colNameOfGroupName) {
        this.colNameOfGroupName = colNameOfGroupName;
    }

    public String getColNameOfTableName() {
        return colNameOfTableName;
    }

    public void setColNameOfTableName(String colNameOfTableName) {
        this.colNameOfTableName = colNameOfTableName;
    }

    public String getColNameOfSelectOrder() {
        return colNameOfSelectOrder;
    }

    public void setColNameOfSelectOrder(String colNameOfSelectOrder) {
        this.colNameOfSelectOrder = colNameOfSelectOrder;
    }

    public String getColNameOfEndValue() {
        return colNameOfEndValue;
    }

    public void setColNameOfEndValue(String colNameOfEndValue) {
        this.colNameOfEndValue = colNameOfEndValue;
    }

    public String getColNameOfCurrentValue() {
        return colNameOfCurrentValue;
    }

    public void setColNameOfCurrentValue(String colNameOfCurrentValue) {
        this.colNameOfCurrentValue = colNameOfCurrentValue;
    }

    public String getColNameOfDeleted() {
        return colNameOfDeleted;
    }

    public void setColNameOfDeleted(String colNameOfDeleted) {
        this.colNameOfDeleted = colNameOfDeleted;
    }

    public String getColNameOfVersion() {
        return colNameOfVersion;
    }

    public void setColNameOfVersion(String colNameOfVersion) {
        this.colNameOfVersion = colNameOfVersion;
    }
}
