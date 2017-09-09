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

import java.io.*;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 08/09/2017.
 */
public class DefaultConfigManager implements ConfigManager {

    private static final boolean FAIL_FAST    = true;
    private static final String  DEFAULT_PATH = System.getProperty("user.home") + "/.ddal/datasource/config.xml";

    private String               location;
    private ConfigClient         client;
    private boolean              failFast     = FAIL_FAST;

    public DefaultConfigManager(String location) {
        this.location = location;
        if (location == null) {
            throw new IllegalArgumentException("location can't be null");
        }
    }

    public DefaultConfigManager(ConfigClient client, boolean failFast) {
        this.client = client;
        this.failFast = failFast;
        this.location = DEFAULT_PATH;
        init(FAIL_FAST);
    }

    public DefaultConfigManager(ConfigClient client, boolean failFast, String location) {
        this.client = client;
        this.failFast = failFast;
        this.location = location;
        init(failFast);
    }

    private void init(boolean failFast) {
        if (client == null) {
            throw new IllegalArgumentException("client can't be null");
        }
        if (location == null) {
            throw new IllegalArgumentException("location can't be null");
        }
        String content = null;
        try {
            content = client.get();
        } catch (Throwable e) {
            if (failFast) {
                throw e;
            } else {//
                e.printStackTrace();
            }
        }
        //
        BufferedWriter bw = null;
        FileWriter fw = null;
        try {
            File file = new File(location);
            File dir = file.getParentFile();
            if (!dir.exists()) {
                dir.mkdirs();
            }
            file.createNewFile();
            fw = new FileWriter(file);
            bw = new BufferedWriter(fw);
            bw.write(content);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            quietClose(fw);
            quietClose(bw);
        }
    }

    private void quietClose(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException ignore) {

            }
        }
    }

    public String getLocation() {
        return this.location;
    }

}
