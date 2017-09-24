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
package org.hellojavaer.ddal.core.utils;

import java.io.*;
import java.net.*;
import java.util.Map;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 09/09/2017.
 */
public class HttpUtils {

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
                    response.append(inputLine + "\n");
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
