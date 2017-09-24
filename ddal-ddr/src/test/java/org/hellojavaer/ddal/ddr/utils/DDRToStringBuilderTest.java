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
package org.hellojavaer.ddal.ddr.utils;

import org.hellojavaer.ddal.core.utils.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 27/12/2016.
 */
public class DDRToStringBuilderTest {

    @Test
    public void test00() {
        DDRToStringBuilder sb = new DDRToStringBuilder().append("a0", "b0")//
                                                        .append("a1", 11)//
                                                        .append("a2", true)//
                                                        .append("a3", null)//
                                                        .append(null, "dd");//
        Assert.equals(sb.toString(), "{\"a0\":\"b0\",\"a1\":11,\"a2\":true,\"a3\":null,null:\"dd\"}");//
        Assert.equals(sb.toString(), "{\"a0\":\"b0\",\"a1\":11,\"a2\":true,\"a3\":null,null:\"dd\"}");//
        sb.append("a4", "12");//
        Assert.equals(sb.toString(), "{\"a0\":\"b0\",\"a1\":11,\"a2\":true,\"a3\":null,null:\"dd\",\"a4\":\"12\"}");//
    }

    @Test
    public void test01() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("a", "b");
        List<Object> list = new ArrayList<Object>();
        list.add(1);
        list.add("2");
        DDRToStringBuilder sb = new DDRToStringBuilder().append("a0", map)//
                                                        .append("a1", list);
        Assert.equals(sb.toString(), "{\"a0\":{\"a\":\"b\"},\"a1\":[1,\"2\"]}");//
    }
}
