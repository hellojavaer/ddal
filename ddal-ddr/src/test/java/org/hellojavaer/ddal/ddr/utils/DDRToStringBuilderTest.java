package org.hellojavaer.ddal.ddr.utils;

import org.junit.Test;

import java.util.*;

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
