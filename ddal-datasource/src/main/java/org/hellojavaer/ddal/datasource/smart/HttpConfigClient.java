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
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 15/07/2017.
 */
public class HttpConfigClient implements ConfigClient {

    private String serverUrl;
    private String clientId;
    private String clientToken;

    public HttpConfigClient(String serverUrl, String clientId, String clientToken) {
        this.serverUrl = serverUrl;
        this.clientId = clientId;
        this.clientToken = clientToken;
    }

    @Override
    public String get() {
        Map<String, Object> params = new HashMap<>();
        params.put("clientId", clientId);
        params.put("clientToken", clientToken);
        return sendPost(serverUrl, params);
    }

    private String sendPost(String url, Map<String, Object> params) {
        try {
            URL obj = new URL(url);
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();
            con.setRequestMethod("POST");
            con.setRequestProperty("User-Agent", "Mozilla/5.0");
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
            BufferedReader in = null;
            try {
                int responseCode = con.getResponseCode();
                if (responseCode != 200) {
                    throw new IllegalStateException();
                }
                in = new BufferedReader(new InputStreamReader(con.getInputStream()));
                String inputLine;
                StringBuilder response = new StringBuilder();
                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                return response.toString();
            } finally {
                closeIO(in);
            }
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    private void closeIO(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {
            }
        }
    }

}
