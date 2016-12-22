/*
 * Copyright 2016-2016 the original author or authors.
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
package org.hellojavaer.ddr.core.datasource.manager.rw;

import java.io.Serializable;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 22/12/2016.
 */
public class SetResult implements Serializable {

    public static final int CODE_OF_SUCCESS                 = 0;
    public static final int CODE_OF_PARAM_ERROR             = 1; // 格式错误,参数必填项为空
    public static final int CODE_OF_ILLEGAL_STATUS          = 0; //
    public static final int CODE_OF_DATA_EXCEPTION          = 0; //
    public static final int CODE_OF_OPERATION_NOT_SUPPORTED = 0;

    private Integer         code;
    private String          desc;

    public SetResult() {
    }

    public SetResult(Integer code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    @Override
    public String toString() {
        return new StringBuilder().append("{code:").append(code).append(",desc:").append(desc).append('}').toString();
    }
}
