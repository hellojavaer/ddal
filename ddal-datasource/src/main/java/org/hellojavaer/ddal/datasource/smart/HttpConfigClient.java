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

import org.hellojavaer.ddal.core.utils.HttpUtils;

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
        return HttpUtils.sendPost(serverUrl, params);
    }

}
