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

import org.hellojavaer.ddal.core.utils.HttpUtils;
import org.hellojavaer.ddal.sequence.exception.GetSequenceFailedException;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 09/09/2017.
 */
public class HttpSequencRangeGetter implements SequenceRangeGetter {

    private String authorizeUrl;
    private String clientId;
    private String clientToken;
    private String accessToken;
    private String accessUrl;

    public HttpSequencRangeGetter(String authorizeUrl, String clientId, String clientToken) {
        this.authorizeUrl = authorizeUrl;
        this.clientId = clientId;
        this.clientToken = clientToken;
    }

    /**
     * param: client_id=&client_token=
     * return: access_url=http&access_token=1&error_code=1
     */
    private void authorize() {
        Map<String, Object> param = new HashMap<>();
        param.put("client_id", clientId);
        param.put("client_token", clientToken);
        String result = HttpUtils.sendPost(this.authorizeUrl, param);
        String[] kvs = result.split("&");
        for (String item : kvs) {
            String[] kv = item.split("=");
            if ("error_code".equals(kv[0]) && kv.length >= 2 && kv[1] != null && kv[1].trim().length() > 0) {
                throw new GetSequenceFailedException("client_id:" + clientId + " authorize failed, return message is: "
                                                     + result);
            } else if ("access_url".equals(kv[0])) {
                this.accessUrl = kv[1];
            } else if ("access_token".equals(kv[0])) {
                this.accessToken = kv[1];
            }
        }
        if (accessUrl != null) {
            accessUrl = accessUrl.trim();
        }
        if (accessToken != null) {
            accessToken = accessToken.trim();
        }
        if (accessUrl == null || accessUrl.length() == 0 || accessToken == null || accessToken.length() == 0) {
            throw new GetSequenceFailedException("client_id:" + clientId + " authorize failed, return message is: "
                                                 + result);
        }
    }

    /**
     * param: client_id=1&access_token=12
     * return: begin_value=1&end_value=2
     *
     */
    @Override
    public SequenceRange get(String schemaName, String tableName, int step) throws Exception {
        Map<String, Object> param = new HashMap<>();
        param.put("client_id", clientId);
        param.put("access_token", accessToken);
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
                    throw new GetSequenceFailedException("client_id:" + clientId
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
}
