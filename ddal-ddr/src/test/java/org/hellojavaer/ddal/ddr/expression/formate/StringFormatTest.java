package org.hellojavaer.ddal.ddr.expression.formate;

import org.hellojavaer.ddal.ddr.expression.format.StringFormat;
import org.hellojavaer.ddal.ddr.utils.Assert;
import org.junit.Test;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 19/11/2016.
 */
public class StringFormatTest {

    /**
     * for input 123
     *  %4s -> 0123
     *  %4S -> 0123
     *  %4[a]s -> a123
     *  %4[a]S -> a123
     *  %4[abc]s -> abc123
     *  %4[abc]S -> c123
     */
    @Test
    public void test0() {
        String input = "123";
        Assert.equals(new StringFormat("%4s").format(input), "0123");
        Assert.equals(new StringFormat("%4S").format(input), "0123");
        Assert.equals(new StringFormat("%4[a]s").format(input), "a123");
        Assert.equals(new StringFormat("%4[a]S").format(input), "a123");
        Assert.equals(new StringFormat("%4[abc]s").format(input), "abc123");
        Assert.equals(new StringFormat("%4[abc]S").format(input), "c123");
    }

    /**
     * for input 123456
     *  %4s -> 123456
     *  %4S -> 3456
     *  %4[a]s -> 123456
     *  %4[a]S -> 3456
     *  %4[abc]s -> 123456
     *  %4[abc]S -> 3456
     *
     */
    @Test
    public void test1() {
        String input = "123456";
        Assert.equals(new StringFormat("%4s").format(input), "123456");
        Assert.equals(new StringFormat("%4S").format(input), "3456");
        Assert.equals(new StringFormat("%4[a]s").format(input), "123456");
        Assert.equals(new StringFormat("%4[a]S").format(input), "3456");
        Assert.equals(new StringFormat("%4[abc]s").format(input), "123456");
        Assert.equals(new StringFormat("%4[abc]S").format(input), "3456");
    }

    /**
     * for input 123
     *  %-4s -> 1230
     *  %-4S -> 1230
     *  %-4[a]s -> 123a
     *  %-4[a]S -> 123a
     *  %-4[abc]s -> 123abc
     *  %-4[abc]S -> 123a
     */
    @Test
    public void test2() {
        String input = "123";
        Assert.equals(new StringFormat("%-4s").format(input), "1230");
        Assert.equals(new StringFormat("%-4S").format(input), "1230");
        Assert.equals(new StringFormat("%-4[a]s").format(input), "123a");
        Assert.equals(new StringFormat("%-4[a]S").format(input), "123a");
        Assert.equals(new StringFormat("%-4[abc]s").format(input), "123abc");
        Assert.equals(new StringFormat("%-4[abc]S").format(input), "123a");
    }

    /**
     * for input 123456
     *  %-4s -> 123456
     *  %-4S -> 1234
     *  %-4[a]s -> 123456
     *  %-4[a]S -> 1234
     *  %-4[abc]s -> 123456
     *  %-4[abc]S -> 1234
     */
    @Test
    public void test3() {
        String input = "123456";
        Assert.equals(new StringFormat("%-4s").format(input), "123456");
        Assert.equals(new StringFormat("%-4S").format(input), "1234");
        Assert.equals(new StringFormat("%-4[a]s").format(input), "123456");
        Assert.equals(new StringFormat("%-4[a]S").format(input), "1234");
        Assert.equals(new StringFormat("%-4[abc]s").format(input), "123456");
        Assert.equals(new StringFormat("%-4[abc]S").format(input), "1234");
    }

    @Test
    public void test20() {
        try {
            StringFormat format = new StringFormat("5s");
            format.format("12");
            Assert.isTrue(false);
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            StringFormat format = new StringFormat("%s");
            format.format("12");
            Assert.isTrue(false);
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            StringFormat format = new StringFormat("%[");
            format.format("12");
            Assert.isTrue(false);
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            StringFormat format = new StringFormat("%]");
            format.format("12");
            Assert.isTrue(false);
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            StringFormat format = new StringFormat("%2[]");
            format.format("12");
            Assert.isTrue(false);
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            StringFormat format = new StringFormat("%2[q]");
            format.format("12");
            Assert.isTrue(false);
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            StringFormat format = new StringFormat("%2[\\m]");
            format.format("12");
            Assert.isTrue(false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test12() {
        StringFormat format = new StringFormat("%6[\\[\\]\\s\\\\]s");
        Assert.isTrue(format.format("123").equals("[] \\123"));
    }

    @Test
    public void test13() {
        try {
            StringFormat format = new StringFormat("%6[\\m]s");
            format.format("12");
            Assert.isTrue(false);
        } catch (Exception e) {
        }
    }

}
