package org.hellojavaer.ddal.ddr.utils;

import org.junit.Test;

import java.util.*;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 23/12/2016.
 */
public class DDRJSONUtilsTest {

    @Test
    public void test01() {
        List<String> list = new ArrayList<>();
        list.add("12");
        list.add("34");
        list.add(null);
        String src = DDRJSONUtils.toJSONString(list);
        Assert.equals(src, "[\"12\",\"34\",null]");
    }

    @Test
    public void test02() {
        List<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(2);
        list.add(null);
        String src = DDRJSONUtils.toJSONString(list);
        Assert.equals(src, "[1,2,null]");
    }

    // list 测试
    @Test
    public void test03() {
        List list = new ArrayList();
        list.add(1);
        list.add("2");
        list.add(null);
        String src = DDRJSONUtils.toJSONString(list);
        Assert.equals(src, "[1,\"2\",null]");
    }

    // map 测试
    @Test
    public void test04() {
        Map map = new LinkedHashMap<>();
        map.put(1, "12");
        map.put("2", 13);
        map.put("2", null);
        map.put(null, 14);
        String src = DDRJSONUtils.toJSONString(map);
        Assert.equals(src, "{1:\"12\",\"2\":null,null:14}");
    }

    // 嵌套测试
    @Test
    public void test05() {
        Map map = new LinkedHashMap<>();
        map.put(1, "12");
        map.put("2", 13);
        List list = new ArrayList();
        list.add("78");
        list.add(90);
        map.put(56, list);
        String src = DDRJSONUtils.toJSONString(map);
        Assert.equals(src, "{1:\"12\",\"2\":13,56:[\"78\",90]}");
    }

    @Test
    public void test06() {
        List list = new ArrayList();
        list.add("78");
        list.add(90);
        Map map = new LinkedHashMap<>();
        map.put(1, "12");
        map.put("2", 13);
        list.add(map);
        String src = DDRJSONUtils.toJSONString(list);
        Assert.equals(src, "[\"78\",90,{1:\"12\",\"2\":13}]");
    }
}
