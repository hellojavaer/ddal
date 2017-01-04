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
package org.hellojavaer.ddr.sequence.db;

import org.hellojavaer.ddr.sequence.IdGetter;
import org.hellojavaer.ddr.sequence.IdRange;

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

    private String          selectSqlTemplate = "SELECT %s, %s FROM %s.%s WHERE %s = ? LIMIT 1";                       //
    private String          updateSqlTemplate = "UPDATE %s.%s SET %s = ?, %s = %s + 1 WHERE %s = ? AND %s = ? LIMIT 1"; //

    private DataSource      dataSource;
    private String          scName;
    private String          tbName;
    private int             sequenceNodes     = 1;

    private String          colNameOfVersion  = "version";
    private String          colNameOfTbName   = "tbName";
    private String          colNameOfIdValue  = "val";

    private volatile String targetSelectSql;
    private volatile String targetUpdateSql;
    private boolean         initialized       = false;

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
                    targetSelectSql = String.format(getSelectSqlTemplate(), colNameOfIdValue, colNameOfVersion, scName,
                                                    tbName, colNameOfTbName);

                    targetUpdateSql = String.format(getUpdateSqlTemplate(), scName, tbName, colNameOfIdValue,
                                                    colNameOfTbName, colNameOfVersion);
                    initialized = true;
                }
            }
        }
    }

    @Override
    public IdRange get(String group, String tabName, int step) throws Exception {
        init();
        Connection connection = null;
        PreparedStatement selectStatement = null;
        PreparedStatement updateStatement = null;
        int rows = 0;
        IdRange idRange = null;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
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
            updateStatement.setLong(1, id + step);
            updateStatement.setLong(2, version);
            rows = updateStatement.executeUpdate();
            connection.commit();
            if (rows > 0) {
                idRange = new IdRange(id, id + step);
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
            if (connection != null) {
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
        this.colNameOfVersion = colNameOfVersion;
    }

    public String getColNameOfTbName() {
        return colNameOfTbName;
    }

    public void setColNameOfTbName(String colNameOfTbName) {
        this.colNameOfTbName = colNameOfTbName;
    }

    public String getColNameOfIdValue() {
        return colNameOfIdValue;
    }

    public void setColNameOfIdValue(String colNameOfIdValue) {
        this.colNameOfIdValue = colNameOfIdValue;
    }
}
