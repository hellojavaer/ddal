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

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ConcurrentModificationException;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 04/01/2017.
 */
public class DatabaseIdGetter implements IdGetter {

    private String          selectSqlTemplate = "SELECT `%s`, `%s` FROM `%s`.`%s` WHERE `%s` = ? LIMIT 1";                           //
    private String          updateSqlTemplate = "UPDATE `%s`.`%s` SET `%s` = ?, `%s` = `%s` + 1 WHERE `%s` = ? AND `%s` = ? LIMIT 1"; //

    private DataSource      dataSource;
    private Connection      connection;
    private String          scName;
    private String          tbName            = "sequence";
    private Integer         sequenceNodes;

    private String          colNameOfVersion  = "version";
    private String          colNameOfTbName   = "tbName";
    private String          colNameOfIdValue  = "val";

    private volatile String targetSelectSql;
    private volatile String targetUpdateSql;
    private boolean         initialized       = false;

    public DatabaseIdGetter() {
    }

    public DatabaseIdGetter(DataSource dataSource, String scName, Integer sequenceNodes) {
        this.dataSource = dataSource;
        this.scName = scName;
        this.sequenceNodes = sequenceNodes;
    }

    public DatabaseIdGetter(Connection connection, String scName, Integer sequenceNodes) {
        this.connection = connection;
        this.scName = scName;
        this.sequenceNodes = sequenceNodes;
    }

    protected String getSelectSqlTemplate() {
        return selectSqlTemplate;
    }

    protected String getUpdateSqlTemplate() {
        return updateSqlTemplate;
    }

    protected void filter(String str) {
        if (str != null && str.indexOf('`') >= 0) {
            throw new IllegalArgumentException("Can't use '`' in parameter '" + str + "'");
        }
    }

    public void init() {
        if (!initialized) {
            synchronized (this) {
                if (!initialized) {
                    if (dataSource != null && connection != null) {
                        throw new IllegalArgumentException(
                                                           "'dataSource' and 'connection', only one of them can be configured");
                    }
                    if (dataSource == null && connection == null) {
                        throw new IllegalArgumentException(
                                                           "'dataSource' and 'connection',one of them should be configured");
                    }
                    if (scName == null) {
                        throw new IllegalArgumentException("'scName' can't be null");
                    }
                    if (tbName == null) {
                        throw new IllegalArgumentException("'tbName' can't be null");
                    }
                    if (sequenceNodes == null || sequenceNodes <= 0) {
                        throw new IllegalArgumentException("'sequenceNodes'[" + sequenceNodes + "] must greater than 0");
                    }
                    targetSelectSql = String.format(getSelectSqlTemplate(), colNameOfIdValue, colNameOfVersion, scName,
                                                    tbName, colNameOfTbName);

                    targetUpdateSql = String.format(getUpdateSqlTemplate(), scName, tbName, colNameOfIdValue,
                                                    colNameOfVersion, colNameOfVersion, colNameOfTbName,
                                                    colNameOfVersion);
                    initialized = true;
                }
            }
        }
    }

    @Override
    public IdRange get(String group, String tabName, int step) throws Exception {
        init();
        Connection connection = this.connection;
        PreparedStatement selectStatement = null;
        PreparedStatement updateStatement = null;
        int rows = 0;
        IdRange idRange = null;
        boolean isNeedCloseConnection = false;
        try {
            if (connection == null) {
                connection = dataSource.getConnection();
                isNeedCloseConnection = true;
                connection.setAutoCommit(false);
            }
            selectStatement = connection.prepareStatement(targetSelectSql);
            selectStatement.setString(1, tbName);
            ResultSet selectResult = selectStatement.executeQuery();
            long id = 0;
            long version = 0;
            if (selectResult.next()) {
                id = selectResult.getLong(1);
                version = selectResult.getLong(2);
            } else {
                return null;
            }
            updateStatement = connection.prepareStatement(targetUpdateSql);
            long endId = id + step * sequenceNodes;
            updateStatement.setLong(1, endId);
            updateStatement.setLong(2, version);
            rows = updateStatement.executeUpdate();
            if(!isNeedCloseConnection) {
                connection.commit();
            }
            if (rows > 0) {
                idRange = new IdRange(id + 1, endId);
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
            if (isNeedCloseConnection && connection != null) {
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
        filter(scName);
        this.scName = scName;
    }

    public String getTbName() {
        return tbName;
    }

    public void setTbName(String tbName) {
        filter(tbName);
        this.tbName = tbName;
    }

    public int getSequenceNodes() {
        return sequenceNodes;
    }

    public void setSequenceNodes(int sequenceNodes) {
        this.sequenceNodes = sequenceNodes;
    }

    public String getColNameOfVersion() {
        return colNameOfVersion;
    }

    public void setColNameOfVersion(String colNameOfVersion) {
        filter(colNameOfVersion);
        this.colNameOfVersion = colNameOfVersion;
    }

    public String getColNameOfTbName() {
        return colNameOfTbName;
    }

    public void setColNameOfTbName(String colNameOfTbName) {
        filter(colNameOfTbName);
        this.colNameOfTbName = colNameOfTbName;
    }

    public String getColNameOfIdValue() {
        return colNameOfIdValue;
    }

    public void setColNameOfIdValue(String colNameOfIdValue) {
        filter(colNameOfIdValue);
        this.colNameOfIdValue = colNameOfIdValue;
    }

}
