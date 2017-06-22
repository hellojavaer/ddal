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
        schema_name varchar(32) NOT NULL,
        table_name varchar(64) NOT NULL,
        begin_value bigint(20) NOT NULL,
        current_value bigint(20) NOT NULL,
        end_value bigint(20) DEFAULT NULL,
        select_order int(11) NOT NULL,
        version bigint(20) NOT NULL DEFAULT '0',
        deleted tinyint(1) NOT NULL DEFAULT '0',
        PRIMARY KEY (id),
        KEY idx_table_name_schema_name_select_order_deleted (table_name,schema_name,select_order,deleted) USING BTREE
     ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
     */

    /*** SELECT id, current_value, end_value, version FROM sc.sequence WHERE schema_name = ? AND table_name = ? AND deleted = 0 ODER BY select_order ASC LIMIT 1 ***/
    private String          selectSqlTemplate     = "SELECT %s, %s, %s, %s FROM %s.%s WHERE %s = ? AND %s = ? AND %s = 0 ORDER BY %s ASC LIMIT 1 ";

    /*** UPDATE sc.sequence SET current_value = ?, deleted = ?, version = version + 1 WHERE id = ? AND version = ? LIMIT 1 ***/
    private String          updateSqlTemplate     = "UPDATE %s.%s SET %s = ?, %s = ?, %s = %s + 1 WHERE %s = ? AND %s = ? LIMIT 1";

    private DataSource      dataSource;
    private Connection      connection;
    private String          scName;
    private String          tbName                = "sequence";

    private String          colNameOfPrimaryKey   = "id";
    private String          colNameOfSchemaName   = "schema_name";
    private String          colNameOfTableName    = "table_name";
    private String          colNameOfCurrentValue = "current_value";
    private String          colNameOfEndValue     = "end_value";
    private String          colNameOfSelectOrder  = "select_order";
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
        init();
    }

    public DatabaseIdGetter(DataSource dataSource, String scName, String tbName) {
        this.dataSource = dataSource;
        this.scName = scName;
        this.tbName = tbName;
        init();
    }

    public DatabaseIdGetter(Connection connection, String scName) {
        this.connection = connection;
        this.scName = scName;
        init();
    }

    public DatabaseIdGetter(Connection connection, String scName, String tbName) {
        this.connection = connection;
        this.scName = scName;
        this.tbName = tbName;
        init();
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
                    Assert.notNull(colNameOfSchemaName, "'colNameOfSchemaName' can't be null");
                    Assert.notNull(colNameOfTableName, "'colNameOfTableName' can't be null");
                    Assert.notNull(colNameOfCurrentValue, "'colNameOfCurrentValue' can't be null");
                    Assert.notNull(colNameOfEndValue, "'colNameOfEndValue' can't be null");
                    Assert.notNull(colNameOfSelectOrder, "'colNameOfSelectOrder' can't be null");
                    Assert.notNull(colNameOfDeleted, "'colNameOfDeleted' can't be null");
                    Assert.notNull(colNameOfVersion, "'colNameOfVersion' can't be null");
                    /*** SELECT id, current_value, end_value, version FROM sc.sequence WHERE schema_name = ? AND table_name = ? AND deleted = 0 ORDER BY select_order ASC LIMIT 1 ***/
                    // "SELECT %s, %s, %s, %s FROM %s.%s WHERE %s = ? AND %s = ? AND %s = 0 ODER BY %s ASC LIMIT 1 ";
                    targetSelectSql = String.format(getSelectSqlTemplate(), colNameOfPrimaryKey, colNameOfCurrentValue,
                                                    colNameOfEndValue,
                                                    colNameOfVersion,//
                                                    scName,
                                                    tbName,//
                                                    colNameOfSchemaName, colNameOfTableName, colNameOfDeleted,
                                                    colNameOfSelectOrder);

                    /*** UPDATE sc.sequence SET current_value = ?, deleted = ?, version = version + 1 WHERE id = ? AND version = ? LIMIT 1 ***/
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
    public IdRange get(String schemaName, String tableName, int step) throws Exception {
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
            selectStatement.setString(1, schemaName);
            selectStatement.setString(2, tableName);
            ResultSet selectResult = selectStatement.executeQuery();
            long id = 0;
            long currentValue = 0;
            Long endValue = null;
            byte deleted = 0;
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
            long nStepsEndValue = currentValue + step;
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
                deleted = 1;
            }
            updateStatement.setLong(1, nStepsEndValue);
            updateStatement.setByte(2, deleted);
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
                    if (deleted != 0) {
                        if (moreThanLimit) {
                            if (logger.isWarnEnabled()) {
                                logger.warn("[More_Than_Limit]Id range for groupName:{},logicalTableName:{} is used up. More detail information is step:{},"
                                                    + "endValue:{},version:{},id:{} and actually allocated range is '{} ~ {}'",
                                            schemaName, tableName, step,//
                                            endValue, version, id, currentValue, oneStepEndValue);
                            }
                        } else {
                            if (logger.isInfoEnabled()) {
                                logger.info("Id range for groupName:{},logicalTableName:{} is used up. More detail information is step:{},"
                                                    + "endValue:{},version:{},id:{} and actually allocated range is '{} ~ {}'",
                                            schemaName, tableName, step,//
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

    public String getColNameOfPrimaryKey() {
        return colNameOfPrimaryKey;
    }

    public void setColNameOfPrimaryKey(String colNameOfPrimaryKey) {
        this.colNameOfPrimaryKey = colNameOfPrimaryKey;
    }

    public String getColNameOfSchemaName() {
        return colNameOfSchemaName;
    }

    public void setColNameOfSchemaName(String colNameOfSchemaName) {
        this.colNameOfSchemaName = colNameOfSchemaName;
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
