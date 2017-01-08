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
 *  <p>Id Range</p>
 *
 *  Design model:
 *
 * _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
 *
 *  |                     --------------
 *                        | 700 ~ NULL |
 * Master                 |            |
 *                        |            |
 *  |                     --------------
 * _ _ _ _ _ _ _ _ _ _ _ _ _/ _ _ _  _\ _ _ _ _ _ _ _ _ _ _ _ _
 *                         /           \
 *  |            -------------        -------------
 *               | 120 ~ 200 |        | 201 ~ 300 |
 * Follower      | 301 ~ 400 |        | 501 ~ 600 |
 *               | 401 ~ 500 |        | 601 ~ 700 |
 *  |            -------------        -------------
 * _ _ _ _ _ _ _ _/_ _ _ _ _ \_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
 *               /            \
 *  |      ------------      -------------
 *         |  1  ~ 20 |      | 21  ~ 40  |
 *         |  41 ~ 60 |      | 81  ~ 100 |
 * Client  |  61 ~ 80 |      | 101 ~ 120 |
 *         ------------      -------------
 *           /    |   \        /    |   \
 *  |      [id] [id]  [id]   [id]  [id] [id]
 * _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
 *
 *  Between master and client,there can be multiple followers. Id Range is firstly allocated by master.
 *  The lower layer request id range to the higher layer and then allocate id range to its lower.
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 04/01/2017.
 */
public class DatabaseIdGetter implements IdGetter {

    private Logger          logger                = LoggerFactory.getLogger(this.getClass());

    /**
     CREATE TABLE sequence (
     id bigint(20) NOT NULL,
     group_name varchar(32) NOT NULL,
     logic_table_name varchar(64) NOT NULL,
     select_order bigint(11) NOT NULL,
     begin_value bigint(11) DEFAULT NULL,
     end_value bigint(11) DEFAULT NULL,
     current_value bigint(11) NOT NULL,
     version bigint(20) NOT NULL DEFAULT '0',
     disabled tinyint(11) NOT NULL DEFAULT '0',
     PRIMARY KEY (id),
     KEY idx_logic_table_name_group_name_select_order_disabled (logic_table_name,group_name,select_order,disabled) USING BTREE
     ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
     */

    /*** SELECT id, current_value, end_value, version FROM sc_name.sequence WHERE group_name = ? AND logic_table_name = ? AND disabled = 0 ODER BY select_oder ASC LIMIT 1 ***/
    private String          selectSqlTemplate     = "SELECT %s, %s, %s, %s FROM %s.%s WHERE %s = ? AND %s = ? AND %s = 0 ORDER BY %s ASC LIMIT 1 ";

    /*** UPDATE sc_name.sequence SET current_value = ?, disabled = ?, version = version + 1 WHERE id = ? AND version = ? LIMIT 1 ***/
    private String          updateSqlTemplate     = "UPDATE %s.%s SET %s = ?, %s = ?, %s = %s + 1 WHERE %s = ? AND %s = ? LIMIT 1";

    private DataSource      dataSource;
    private Connection      connection;
    private String          scName;
    private String          tbName                = "sequence";
    private Integer         skipNSteps            = 0;

    private String          colNameOfPrimaryKey   = "id";
    private String          colNameOfGroupName    = "group_name";
    private String          colNameOfTableName    = "logic_table_name";
    private String          colNameOfSelectOrder  = "select_order";
    private String          colNameOfEndValue     = "end_value";
    private String          colNameOfCurrentValue = "current_value";
    private String          colNameOfDeleted      = "disabled";
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
                    Assert.isTrue(skipNSteps >= 0, "'skipNSteps' must greater than or equal to 0");
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

                    /*** SELECT id, current_value, end_value, version FROM sc_name.sequence WHERE group_name = ? AND logic_table_name = ? AND disabled = 0 ORDER BY select_oder ASC LIMIT 1 ***/
                    // "SELECT %s, %s, %s, %s FROM %s.%s WHERE %s = ? AND %s = ? AND %s = 0 ODER BY %s ASC LIMIT 1 ";
                    targetSelectSql = String.format(getSelectSqlTemplate(), colNameOfPrimaryKey, colNameOfCurrentValue,
                                                    colNameOfEndValue,
                                                    colNameOfVersion,//
                                                    scName,
                                                    tbName,//
                                                    colNameOfGroupName, colNameOfTableName, colNameOfDeleted,
                                                    colNameOfSelectOrder);

                    /*** UPDATE sc_name.sequence SET current_value = ?, disabled = ?, version = version + 1 WHERE id = ? AND version = ? LIMIT 1 ***/
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
    public IdRange get(String groupName, String logicTableName, int step) throws Exception {
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
            selectStatement.setString(2, logicTableName);
            ResultSet selectResult = selectStatement.executeQuery();
            long id = 0;
            long currentValue = 0;
            Long endValue = null;
            int disabled = 0;
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
            long oneStepEndValue = currentValue + step;
            long nStepsEndValue = currentValue + step * (skipNSteps + 1);
            boolean moreThanLimit = false;
            if (endValue != null) {
                if (currentValue >= endValue) {
                    nStepsEndValue = currentValue;
                } else if (nStepsEndValue >= endValue) {
                    if (nStepsEndValue > endValue) {
                        nStepsEndValue = endValue;
                        moreThanLimit = true;
                    }
                }
                disabled = 1;
            }
            updateStatement.setLong(1, nStepsEndValue);
            updateStatement.setLong(2, disabled);
            updateStatement.setLong(3, id);
            updateStatement.setLong(4, version);
            rows = updateStatement.executeUpdate();
            if (isNewConnection) {
                connection.commit();
            }
            if (rows > 0) {
                if (endValue != null && currentValue >= endValue) {
                    return null;
                } else {
                    idRange = new IdRange(currentValue + 1, oneStepEndValue);
                    if (logger.isInfoEnabled()) {
                        logger.info("[Get_Range] " + idRange);
                    }
                    if (disabled != 0) {
                        if (moreThanLimit) {
                            if (logger.isWarnEnabled()) {
                                logger.warn("[More_Than_Limit]Id range for groupName:{},logicTableName:{} is used up. More detail information is step:{},skipNSteps:{},"
                                                    + "endValue:{},version:{},id:{} and actually allocated range is '{} ~ {}'",
                                            groupName, logicTableName, step, skipNSteps,//
                                            endValue, version, id, currentValue, oneStepEndValue);
                            }
                        } else {
                            if (logger.isInfoEnabled()) {
                                logger.info("Id range for groupName:{},logicTableName:{} is used up. More detail information is step:{},skipNSteps:{},"
                                                    + "endValue:{},version:{},id:{} and actually allocated range is '{} ~ {}'",
                                            groupName, logicTableName, step, skipNSteps,//
                                            endValue, version, id, currentValue, oneStepEndValue);
                            }
                        }
                    }
                }
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
