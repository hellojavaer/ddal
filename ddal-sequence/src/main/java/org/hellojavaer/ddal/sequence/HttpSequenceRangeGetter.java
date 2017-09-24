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

import org.hellojavaer.ddal.sequence.exception.GetSequenceFailedException;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 09/09/2017.
 */
public class HttpSequenceRangeGetter implements SequenceRangeGetter {

    private String authorizeUrl;
    private String clientId;
    private String authorizeToken;
    private String accessToken;
    private String accessUrl;

    public HttpSequenceRangeGetter(String authorizeUrl, String clientId, String authorizeToken) {
        this.authorizeUrl = authorizeUrl;
        this.clientId = clientId;
        this.authorizeToken = authorizeToken;
        authorize();
    }

    /**
     * param: client_id=1&access_token=12
     * return: begin_value=1&end_value=2
     */
    @Override
    public SequenceRange get(String schemaName, String tableName, int step) throws Exception {
        Map<String, String> param = new HashMap<>();
        param.put("client_id", clientId);
        param.put("access_token", accessToken);
        param.put("schema_name", schemaName);
        param.put("table_name", tableName);
        param.put("step", String.valueOf(step));
        String result = HttpUtils.sendPost(accessUrl, param);
        String[] kvs = result.split("&");
        Long beginValue = null;
        Long endValue = null;
        boolean hasErrorCode = false;
        for (String item : kvs) {
            String[] kv = item.split("=");
            if ("error_code".equals(kv[0])) {
                hasErrorCode = true;
                break;
            } else if ("begin_value".equals(kv[0])) {
                beginValue = Long.valueOf(kv[1].trim());
            } else if ("end_value".equals(kv[0])) {
                endValue = Long.valueOf(kv[1].trim());
            }
        }
        if (hasErrorCode) {
            authorize();
            result = HttpUtils.sendPost(accessUrl, param);
            kvs = result.split("&");
            for (String item : kvs) {
                String[] kv = item.split("=");
                if ("error_code".equals(kv[0])) {
                    throw new GetSequenceFailedException("clientId:" + clientId
                                                         + " access data failed, return message is:" + result);
                } else if ("begin_value".equals(kv[0])) {
                    beginValue = Long.valueOf(kv[1].trim());
                } else if ("end_value".equals(kv[0])) {
                    endValue = Long.valueOf(kv[1].trim());
                }
            }
        }
        SequenceRange sequenceRange = new SequenceRange();
        sequenceRange.setBeginValue(beginValue);
        sequenceRange.setEndValue(endValue);
        return sequenceRange;
    }

    /**
     * param: client_id=&client_token=
     * return: access_url=http&access_token=1&error_code=1
     */
    private void authorize() {
        Map<String, String> param = new HashMap<>();
        param.put("client_id", clientId);
        param.put("authorize_token", authorizeToken);
        String result = HttpUtils.sendPost(authorizeUrl, param);
        String[] kvs = result.split("&");
        for (String item : kvs) {
            String[] kv = item.split("=");
            if ("error_code".equals(kv[0]) && kv.length >= 2 && kv[1] != null && kv[1].trim().length() > 0) {
                throw new GetSequenceFailedException("clientId:" + clientId + " authorize failed, return message is: "
                                                     + result);
            } else if ("access_url".equals(kv[0])) {
                this.accessUrl = kv[1];
            } else if ("access_token".equals(kv[0])) {
                this.accessToken = kv[1];
            }
        }
        if (accessUrl != null) {
            try {
                accessUrl = URLDecoder.decode(accessUrl.trim(), "UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }
        if (accessToken != null) {
            accessToken = accessToken.trim();
        }
        if (accessUrl == null || accessUrl.length() == 0 || accessToken == null || accessToken.length() == 0) {
            throw new GetSequenceFailedException("clientId:" + clientId + " authorize failed, return message is: "
                                                 + result);
        }
    }

    static class HttpUtils {

        private static final String USER_AGENT = "Mozilla/5.0";

        public static String sendPost(String url, Map<String, String> params) {
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
                        for (Map.Entry<String, String> entry : params.entrySet()) {
                            sb.append(encode(entry.getKey()));
                            sb.append('=');
                            sb.append(encode(entry.getValue()));
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

        private static String encode(String str) {
            if (str == null) {
                return "";
            }
            try {
                return URLEncoder.encode(str, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
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
