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
package org.hellojavaer.ddal.datasource;

import org.hellojavaer.ddal.ddr.datasource.jdbc.DDRDataSource;
import org.hellojavaer.ddal.ddr.shard.ShardRouter;
import org.hellojavaer.ddal.sequence.Sequence;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;

import javax.sql.DataSource;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 22/08/2017.
 */
public class DefaultDDALDataSource implements DDALDataSource {

    private static final String JDBC_DDAL_PROTOCOL_PREFIX = "jdbc:ddal:";
    private static final String THICK_PROTOCOL_PREFIX     = "thick:";
    private static final String THIN_PROTOCOL_PREFIX      = "thin:";

    @Qualifier
    private DataSource          dataSource;
    private Sequence            sequence;
    private ShardRouter         shardRouter;

    public DefaultDDALDataSource(String url) {
        this(url, null, null);
    }

    /**
     *
     * jdbc:ddal:thick:
     * 
     * jdbc:ddal:thin:
     * 
     */
    public DefaultDDALDataSource(String url, String username, String password) {
        if (url != null) {
            url = url.trim();
        }
        if (url == null || url.length() == 0) {
            throw new IllegalArgumentException("url can't be null");
        }
        if (!url.startsWith(JDBC_DDAL_PROTOCOL_PREFIX)) {
            throw new IllegalArgumentException("url must be start with '" + JDBC_DDAL_PROTOCOL_PREFIX + "'");
        }
        String url1 = url.substring(JDBC_DDAL_PROTOCOL_PREFIX.length()).trim();
        ApplicationContext context;
        if (url1.startsWith(THICK_PROTOCOL_PREFIX)) {
            String url2 = url1.substring(THICK_PROTOCOL_PREFIX.length());
            if (url2.startsWith("classpath:") || url2.startsWith("classpath*:")) {
                context = new ClassPathXmlApplicationContext(url2);
            } else if (url2.startsWith("file:")) {
                context = new FileSystemXmlApplicationContext(url2);
            } else if (url2.startsWith("//")) {
                url2 = "http:" + url2;
                Map<String, Object> param = new LinkedHashMap<>();
                param.put("username", username);
                param.put("password", password);
                String content = HttpUtils.sendPost(url2, param);
                Resource resource = new ByteArrayResource(content.getBytes());
                GenericXmlApplicationContext genericXmlApplicationContext = new GenericXmlApplicationContext();
                genericXmlApplicationContext.load(resource);
                genericXmlApplicationContext.refresh();
                context = genericXmlApplicationContext;
            } else {
                throw new IllegalArgumentException("Unsupported protocol " + url);
            }
        } else if (url1.startsWith(THIN_PROTOCOL_PREFIX)) {
            // TODOD
            throw new IllegalArgumentException("Unsupported protocol jdbc:ddal:thin:");
        } else {
            throw new IllegalArgumentException("Unsupported protocol " + url);
        }
        this.dataSource = getBean(context, DDRDataSource.class, "ddrDataSource");
        this.sequence = getBean(context, Sequence.class, "sequence");
        this.shardRouter = getBean(context, ShardRouter.class, "shardRouter");
    }

    private <T> T getBean(ApplicationContext context, Class<T> requiredType, String beanName) {
        Map<String, T> map = context.getBeansOfType(requiredType);
        if (map == null || map.isEmpty()) {
            return null;
        }
        if (map.size() == 1) {
            return map.values().iterator().next();
        } else {
            if (beanName != null) {
                T t = map.get(beanName);
                if (t != null) {
                    return t;
                }
            }
            throw new NoUniqueBeanDefinitionException(requiredType, map.keySet());
        }
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

    static class HttpUtils {

        private static final String USER_AGENT = "Mozilla/5.0";

        public static String sendPost(String url, Map<String, Object> params) {
            try {
                URL obj = new URL(url);
                HttpURLConnection con = (HttpURLConnection) obj.openConnection();
                con.setRequestMethod("POST");
                con.setRequestProperty("User-Agent", USER_AGENT);
                con.setRequestProperty("Accept-Language", "en-US,en;q=0.5");
                con.setDoOutput(true);
                // send
                DataOutputStream wr = null;
                try {
                    wr = new DataOutputStream(con.getOutputStream());
                    if (params != null && !params.isEmpty()) {
                        StringBuilder sb = new StringBuilder();
                        for (Map.Entry<String, Object> entry : params.entrySet()) {
                            sb.append(URLEncoder.encode(entry.getKey(), "UTF-8"));
                            sb.append('=');
                            sb.append(URLEncoder.encode(entry.getValue().toString(), "UTF-8"));
                            sb.append('&');
                        }
                        sb.deleteCharAt(sb.length() - 1);
                        wr.write(sb.toString().getBytes("UTF-8"));
                    }
                    wr.flush();
                } finally {
                    closeIO(wr);
                }
                // get
                BufferedReader br = null;
                InputStream in;
                if (con.getResponseCode() < HttpURLConnection.HTTP_BAD_REQUEST) {
                    in = con.getInputStream();
                } else {
                    in = con.getErrorStream();
                }
                try {
                    br = new BufferedReader(new InputStreamReader(in));
                    String inputLine;
                    StringBuilder response = new StringBuilder();
                    while ((inputLine = br.readLine()) != null) {
                        response.append(inputLine);
                        response.append('\n');
                    }
                    int responseCode = con.getResponseCode();
                    if (responseCode != 200) {
                        throw new IllegalStateException("http code " + responseCode + "\n" + response.toString());
                    } else {
                        return response.toString();
                    }
                } finally {
                    closeIO(br);
                }
            } catch (Exception e) {
                if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                } else {
                    throw new RuntimeException(e);
                }
            }
        }

        private static void closeIO(Closeable closeable) {
            if (closeable != null) {
                try {
                    closeable.close();
                } catch (IOException e) {
                }
            }
        }
    }
}
