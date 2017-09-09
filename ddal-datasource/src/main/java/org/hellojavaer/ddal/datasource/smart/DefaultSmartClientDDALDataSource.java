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
package org.hellojavaer.ddal.datasource.smart;

import org.hellojavaer.ddal.datasource.SmartClientDDALDataSource;
import org.hellojavaer.ddal.ddr.datasource.jdbc.DDRDataSource;
import org.hellojavaer.ddal.ddr.shard.ShardRouter;
import org.hellojavaer.ddal.sequence.Sequence;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 22/08/2017.
 */
public class DefaultSmartClientDDALDataSource implements SmartClientDDALDataSource {

    private DataSource    dataSource;
    private ConfigManager configManager;
    private Sequence      sequence;
    private ShardRouter   shardRouter;

    public DefaultSmartClientDDALDataSource(ConfigManager configManager) {
        this.configManager = configManager;
        init();
    }

    public DefaultSmartClientDDALDataSource(ConfigManager configManager, Sequence sequence) {
        this.configManager = configManager;
        this.sequence = sequence;
        init();
    }

    private void init() {
        String location = configManager.getLocation();
        ApplicationContext context = null;
        if (location.startsWith("classpath:") || location.startsWith("classpath*:")) {
            context = new ClassPathXmlApplicationContext(location);
        } else {
            context = new FileSystemXmlApplicationContext(location);
        }
        this.dataSource = context.getBean(DDRDataSource.class, "ddrDataSource");
        this.sequence = context.getBean(Sequence.class, "sequence");
        this.shardRouter = context.getBean(ShardRouter.class, "shardRouter");
    }

    protected DataSource getDataSource() {
        return dataSource;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return getDataSource().getConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return getDataSource().getConnection(username, password);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return getDataSource().unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return getDataSource().isWrapperFor(iface);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return getDataSource().getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        getDataSource().setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        getDataSource().setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return getDataSource().getLoginTimeout();
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return getDataSource().getParentLogger();
    }

    @Override
    public Sequence getSequence() {
        return sequence;
    }

    @Override
    public ShardRouter getShardRouter() {
        return shardRouter;
    }

}
