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
import java.util.Collections;
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
     * param: clientId= &accessToken= &schemaName= &tableName= &step=
     * return: beginValue= &endValue= &errorCode= &errorMessage=
     */
    @Override
    public SequenceRange get(String schemaName, String tableName, int step) throws Exception {
        Map<String, String> param = new HashMap<>();
        param.put("clientId", clientId);
        param.put("accessToken", accessToken);
        param.put("schemaName", schemaName);
        param.put("tableName", tableName);
        param.put("step", String.valueOf(step));
        String result = HttpUtils.sendPost(accessUrl, param);
        Map<String, String> resultMap = parseHttpKvString(result);
        if (resultMap.isEmpty()) {
            return null;
        } else {
            if (resultMap.get("errorCode") != null) {
                authorize();
                result = HttpUtils.sendPost(accessUrl, param);
                resultMap = parseHttpKvString(result);
                if (resultMap.get("errorCode") != null) {
                    throw new GetSequenceFailedException("clientId:" + clientId
                                                         + " access data failed, return message is:" + result);
                }
            }
            SequenceRange sequenceRange = new SequenceRange();
            sequenceRange.setBeginValue(parseLong(resultMap.get("beginValue")));
            sequenceRange.setEndValue(parseLong(resultMap.get("endValue")));
            return sequenceRange;
        }
    }

    /**
     * param: clientId= &clientToken=
     * return: accessUrl= &accessToken= &errorCode= &errorMessage=
     */
    private void authorize() {
        Map<String, String> param = new HashMap<>();
        param.put("clientId", clientId);
        param.put("authorizeToken", authorizeToken);
        String result = HttpUtils.sendPost(authorizeUrl, param);
        Map<String, String> resultMap = parseHttpKvString(result);
        if (resultMap.isEmpty()) {
            throw new GetSequenceFailedException("clientId:" + clientId + " authorize failed, return message is empty");
        } else {
            if (resultMap.get("errorCode") != null) {
                throw new GetSequenceFailedException("clientId:" + clientId + " authorize failed, return message is: "
                                                     + result);
            }
            String accessUrl = resultMap.get("accessUrl");
            String accessToken = resultMap.get("accessToken");
            if (accessUrl == null || accessToken == null) {
                throw new GetSequenceFailedException(
                                                     "clientId:"
                                                             + clientId
                                                             + " authorize failed, accessUrl or accessToken is null, detail message is "
                                                             + result);
            }
            try {
                accessUrl = URLDecoder.decode(accessUrl.trim(), "UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new GetSequenceFailedException(e);
            }
            this.accessUrl = accessUrl;
            this.accessToken = accessToken;
        }
    }

    private Map<String, String> parseHttpKvString(String string) {
        if (string == null) {
            return Collections.emptyMap();
        }
        string = string.trim();
        if (string.length() == 0) {
            return Collections.emptyMap();
        }
        Map<String, String> map = new HashMap<>();
        String[] kvs = string.split("&");
        for (String kvString : kvs) {
            String[] kv = kvString.split("=");
            if (kv.length == 2) {
                String val = kv[1];
                val = val.trim();
                if (val.length() == 0) {
                    val = null;
                }
                map.put(kv[0], val);
            } else if (kv.length == 1) {
                map.put(kv[0], null);
            } else {
                throw new RuntimeException("Illegaled http kv string:" + kvString);
            }
        }
        return map;
    }

    private Long parseLong(String str) {
        if (str == null) {
            return null;
        }
        str = str.trim();
        if (str.length() == 0) {
            return null;
        }
        return Long.parseLong(str);
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
                        throw new IllegalStateException("the requested url: " + url + " returned error code: "
                                                        + responseCode + "\n" + response.toString());
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
