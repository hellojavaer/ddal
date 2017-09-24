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
package org.hellojavaer.ddal.sequence;

import org.hellojavaer.ddal.core.utils.Assert;
import org.hellojavaer.ddal.sequence.exception.IllegalSequenceRangeException;
import org.hellojavaer.ddal.sequence.exception.NoAvailableSequenceRangeFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ConcurrentModificationException;

/**
 *  <p>Sequence Range</p>
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
 *  Between master and client,there can be multiple followers. Sequence range is firstly allocated by master.
 *  The lower layer request sequence range to the higher layer and then allocate sequence range to its lower.
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 04/01/2017.
 */
public class DatabaseSequenceRangeGetter implements SequenceRangeGetter {

    private Logger          logger               = LoggerFactory.getLogger(this.getClass());

    /**
     CREATE TABLE sequence (
        id bigint(20) NOT NULL AUTO_INCREMENT,
        schema_name varchar(32) NOT NULL,
        table_name varchar(64) NOT NULL,
        begin_value bigint(20) NOT NULL,
        next_value bigint(20) NOT NULL,
        end_value bigint(20) DEFAULT NULL,
        step int(11),
        skip_n_steps int(11),
        select_order int(11) NOT NULL,
        version bigint(20) NOT NULL DEFAULT '0',
        deleted tinyint(1) NOT NULL DEFAULT '0',
        PRIMARY KEY (id),
        KEY idx_table_name_schema_name_deleted_select_order (table_name,schema_name,deleted,select_order) USING BTREE
     ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
     */

    /*** SELECT id, next_value, end_value, step, skip_n_steps, version FROM sc.sequence WHERE schema_name = ? AND table_name = ? AND deleted = 0 ODER BY select_order ASC LIMIT 1 ***/
    private String          selectSqlTemplate    = "SELECT %s, %s, %s, %s, %s, %s FROM %s.%s WHERE %s = ? AND %s = ? AND %s = 0 ORDER BY %s ASC LIMIT 1 ";

    /*** UPDATE sc.sequence SET next_value = ?, deleted = ?, version = version + 1 WHERE id = ? AND version = ? LIMIT 1 ***/
    private String          updateSqlTemplate    = "UPDATE %s.%s SET %s = ?, %s = ?, %s = %s + 1 WHERE %s = ? AND %s = ? LIMIT 1";

    private DataSource      dataSource;
    private Connection      connection;
    private String          scName;
    private String          tbName               = "sequence";

    private String          colNameOfPrimaryKey  = "id";
    private String          colNameOfSchemaName  = "schema_name";
    private String          colNameOfTableName   = "table_name";
    private String          colNameOfNextValue   = "next_value";
    private String          colNameOfEndValue    = "end_value";
    private String          colNameOfStep        = "step";                                                                                                //
    private String          colNameOfSkipNSteps  = "skip_n_steps";                                                                                        //
    private String          colNameOfSelectOrder = "select_order";
    private String          colNameOfDeleted     = "deleted";
    private String          colNameOfVersion     = "version";

    private volatile String targetSelectSql;
    private volatile String targetUpdateSql;
    private boolean         initialized          = false;

    public DatabaseSequenceRangeGetter() {
    }

    public DatabaseSequenceRangeGetter(DataSource dataSource, String scName) {
        this.dataSource = dataSource;
        this.scName = scName;
        init();
    }

    public DatabaseSequenceRangeGetter(DataSource dataSource, String scName, String tbName) {
        this.dataSource = dataSource;
        this.scName = scName;
        this.tbName = tbName;
        init();
    }

    public DatabaseSequenceRangeGetter(Connection connection, String scName) {
        this.connection = connection;
        this.scName = scName;
        init();
    }

    public DatabaseSequenceRangeGetter(Connection connection, String scName, String tbName) {
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
                    Assert.notNull(colNameOfNextValue, "'colNameOfNextValue' can't be null");
                    Assert.notNull(colNameOfEndValue, "'colNameOfEndValue' can't be null");
                    Assert.notNull(colNameOfStep, "'colNameOfStep' can't be null");
                    Assert.notNull(colNameOfSkipNSteps, "'colNameOfSkipNSteps' can't be null");
                    Assert.notNull(colNameOfSelectOrder, "'colNameOfSelectOrder' can't be null");
                    Assert.notNull(colNameOfDeleted, "'colNameOfDeleted' can't be null");
                    Assert.notNull(colNameOfVersion, "'colNameOfVersion' can't be null");
                    /*** SELECT id, next_value, end_value, step, skip_n_steps, version FROM sc.sequence WHERE schema_name = ? AND table_name = ? AND deleted = 0 ORDER BY select_order ASC LIMIT 1 ***/
                    // "SELECT %s, %s, %s, %s, %s, %s FROM %s.%s WHERE %s = ? AND %s = ? AND %s = 0 ODER BY %s ASC LIMIT 1 ";
                    targetSelectSql = String.format(getSelectSqlTemplate(), colNameOfPrimaryKey,
                                                    colNameOfNextValue,//
                                                    colNameOfEndValue,//
                                                    colNameOfStep,//
                                                    colNameOfSkipNSteps,//
                                                    colNameOfVersion,//
                                                    scName,
                                                    tbName,//
                                                    colNameOfSchemaName, colNameOfTableName, colNameOfDeleted,
                                                    colNameOfSelectOrder);

                    /*** UPDATE sc.sequence SET next_value = ?, deleted = ?, version = version + 1 WHERE id = ? AND version = ? LIMIT 1 ***/
                    // "UPDATE %s.%s SET %s = ?, %s = ?, %s = %s + 1 WHERE %s = ? AND %s = ? LIMIT 1";
                    targetUpdateSql = String.format(getUpdateSqlTemplate(), scName, tbName, colNameOfNextValue,
                                                    colNameOfDeleted, colNameOfVersion, colNameOfVersion,
                                                    colNameOfPrimaryKey, colNameOfVersion);
                    initialized = true;
                }
            }
        }
    }

    @Override
    public SequenceRange get(String schemaName, String tableName, int step) throws Exception {
        init();
        int rows = 0;
        SequenceRange sequenceRange = null;
        Connection connection = this.connection;
        PreparedStatement selectStatement = null;
        PreparedStatement updateStatement = null;
        boolean isNewConnection = false;
        if (connection == null || connection.isClosed()) {
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
            long nextValue = 0;
            Long endValue = null;
            Integer dbStep = null;
            Integer skipNSteps = null;
            byte deleted = 0;
            long version = 0;
            if (selectResult.next()) {//
                id = ((Number) selectResult.getObject(1)).longValue();
                nextValue = ((Number) selectResult.getLong(2)).longValue();
                Object endValueObj = selectResult.getObject(3);
                if (endValueObj != null) {
                    endValue = ((Number) endValueObj).longValue();
                }
                Object stepObj = selectResult.getObject(4);
                if (stepObj != null) {
                    dbStep = ((Number) stepObj).intValue();
                }
                Object skipNStepsObj = selectResult.getObject(5);
                if (skipNStepsObj != null) {
                    skipNSteps = ((Number) skipNStepsObj).intValue();
                }
                version = ((Number) selectResult.getLong(6)).longValue();
            } else {
                throw new NoAvailableSequenceRangeFoundException("No available sequence rang was found for schemaName:'"
                                                                 + schemaName + "', tableName:'" + tableName + "'");
            }
            // 数据库步长优先
            if (dbStep != null && dbStep > 0) {
                step = dbStep;
            }
            long rangeBegin = nextValue;
            long rangeEnd = nextValue + step - 1;
            if (endValue != null && rangeEnd > endValue) {
                rangeEnd = endValue;
            }
            updateStatement = connection.prepareStatement(targetUpdateSql);
            boolean dirtyRow = false;
            if (endValue != null && nextValue > endValue) {// 兼容异常数据
                deleted = 1;
                dirtyRow = true;
            } else {
                if (dbStep != null && dbStep > 0 && skipNSteps != null && skipNSteps > 0) {
                    nextValue = nextValue + step * (skipNSteps + 1);
                } else {
                    nextValue = nextValue + step;
                }
                if (endValue != null && nextValue > endValue) {
                    nextValue = endValue + 1;
                    deleted = 1;
                }
            }
            updateStatement.setLong(1, nextValue);
            updateStatement.setByte(2, deleted);
            updateStatement.setLong(3, id);
            updateStatement.setLong(4, version);
            rows = updateStatement.executeUpdate();
            if (isNewConnection) {
                connection.commit();
            }
            if (rows > 0) {
                if (dirtyRow) {// 兼容异常数据
                    throw new IllegalSequenceRangeException(
                                                      String.format("Illegal sequence range {id:%s, nextValue:%s, endValue:%s, version:%s, deleted:0}",
                                                                    id, nextValue, endValue, version));
                }
                if (logger.isInfoEnabled()) {
                    logger.info("[Get_Range]: " + sequenceRange);
                }
                if (deleted == 1) {
                    if (logger.isInfoEnabled()) {
                        logger.info("Sequence range for schemaName:{},tableName:{} is used up. More detail information is step:{},"
                                            + "endValue:{},version:{},id:{} and actually allocated range is '{} ~ {}'",
                                    schemaName, tableName, step,//
                                    endValue, version, id, rangeBegin, rangeEnd);
                    }
                }
                return new SequenceRange(rangeBegin, rangeEnd);
            } else {
                throw new ConcurrentModificationException();
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

    public String getColNameOfStep() {
        return colNameOfStep;
    }

    public void setColNameOfStep(String colNameOfStep) {
        this.colNameOfStep = colNameOfStep;
    }

    public String getColNameOfSkipNSteps() {
        return colNameOfSkipNSteps;
    }

    public void setColNameOfSkipNSteps(String colNameOfSkipNSteps) {
        this.colNameOfSkipNSteps = colNameOfSkipNSteps;
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

    public String getColNameOfNextValue() {
        return colNameOfNextValue;
    }

    public void setColNameOfNextValue(String colNameOfNextValue) {
        this.colNameOfNextValue = colNameOfNextValue;
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
