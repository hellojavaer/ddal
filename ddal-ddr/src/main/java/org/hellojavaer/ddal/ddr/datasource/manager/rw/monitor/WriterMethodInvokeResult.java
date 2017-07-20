/*
 * Copyright 2016-2017 the original author or authors.
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
package org.hellojavaer.ddal.ddr.datasource.manager.rw.monitor;

import org.hellojavaer.ddal.ddr.utils.DDRToStringBuilder;

import java.io.Serializable;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 22/12/2016.
 */
public class WriterMethodInvokeResult implements Serializable {

    private static final long serialVersionUID         = 0L;

    public static final int   CODE_OF_SUCCESS          = 0;
    public static final int   CODE_OF_ILLEGAL_ARGUMENT = 20; // 格式错误,参数必填项为空
    public static final int   CODE_OF_DATA_IS_EMPTY    = 40; //
    // public static final int CODE_OF_OPERATION_NOT_SUPPORTED = 60;

    private Integer           code;
    private String            desc;

    public WriterMethodInvokeResult() {
    }

    public WriterMethodInvokeResult(Integer code, String desc) {
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
        return new DDRToStringBuilder().append("code", code).append("desc", desc).toString();
    }
}
