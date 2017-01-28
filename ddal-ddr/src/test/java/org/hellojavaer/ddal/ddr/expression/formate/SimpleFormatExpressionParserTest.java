package org.hellojavaer.ddal.ddr.expression.formate;

import org.hellojavaer.ddal.ddr.expression.format.FormatExpression;
import org.hellojavaer.ddal.ddr.expression.format.FormatExpressionContext;
import org.hellojavaer.ddal.ddr.expression.format.simple.SimpleFormatExpressionContext;
import org.hellojavaer.ddal.ddr.expression.format.simple.SimpleFormatExpressionParser;
import org.hellojavaer.ddal.ddr.utils.Assert;
import org.junit.Test;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 19/11/2016.
 */
public class SimpleFormatExpressionParserTest {

    @Test
    public void test0() {
        SimpleFormatExpressionParser s = new SimpleFormatExpressionParser();
        FormatExpression fe = s.parse("tab_{'aa':'%4s'}_name");
        FormatExpressionContext context = new SimpleFormatExpressionContext();
        Assert.equals(fe.getValue(context), "tab_00aa_name");

        FormatExpression fe1 = s.parse("tab_{\"bb\":'%4s'}_name");
        Assert.equals(fe1.getValue(null), "tab_00bb_name");
    }

    @Test
    public void test1() {
        SimpleFormatExpressionParser s = new SimpleFormatExpressionParser();
        FormatExpression fe = s.parse("tab_{var:'%6s'}_name");
        FormatExpressionContext context = new SimpleFormatExpressionContext();
        context.setVariable("var", 12);
        Assert.equals(fe.getValue(context), "tab_000012_name");
    }

    @Test
    public void test00() {
        SimpleFormatExpressionParser s = new SimpleFormatExpressionParser();
        FormatExpression fe = s.parse("tab_{$0:'%4s'}_name {$1:'%6s'}");
        FormatExpressionContext context = new SimpleFormatExpressionContext();
        context.setVariable("$0", 78);
        context.setVariable("$1", null);
        Assert.equals(fe.getValue(context), "tab_0078_name 00null");
    }

    @Test
    public void test01() {
        SimpleFormatExpressionParser s = new SimpleFormatExpressionParser();
        FormatExpression fe = s.parse("ab_{$0:'%4s'}  cd_{$1:'%4s'}_ef");
        FormatExpressionContext context = new SimpleFormatExpressionContext();
        context.setVariable("$0", 12);
        context.setVariable("$1", 34);
        Assert.equals(fe.getValue(context), "ab_0012  cd_0034_ef");
    }

    @Test
    public void test02() {
        SimpleFormatExpressionParser s = new SimpleFormatExpressionParser();
        FormatExpression fe = s.parse("ab_{$0:'%4s'}_cd_{$1:'%4s'}_ef");
        FormatExpressionContext context = new SimpleFormatExpressionContext();
        context.setVariable("$0", 12);
        context.setVariable("$1", 34);
        Assert.equals(fe.getValue(context), "ab_0012_cd_0034_ef");
    }

    @Test
    public void test03() {
        SimpleFormatExpressionParser s = new SimpleFormatExpressionParser();
        FormatExpression fe = s.parse("tab_{$0 :'%5[i]s':'%8[m]s'}_name");
        FormatExpressionContext context = new SimpleFormatExpressionContext();
        context.setVariable("$0", 78);
        Assert.equals(fe.getValue(context), "tab_mmmiii78_name");
    }

    /**
     *
     * tab_{ $0:'%6["\'{}]s'}_name
     *
     * tab_"'{}78_name
     */
    @Test
    public void test13() {
        SimpleFormatExpressionParser s = new SimpleFormatExpressionParser();
        FormatExpression fe = s.parse("tab_{ $0:'%6[\"\\'{}]s'}_name");
        FormatExpressionContext context = new SimpleFormatExpressionContext();
        context.setVariable("$0", 78);
        Assert.equals(fe.getValue(context), "tab_\"'{}78_name");
    }

    /**
     *
     * tab_{ $0:'%6[\\[]s'}_name
     */
    @Test
    public void test14() {
        SimpleFormatExpressionParser s = new SimpleFormatExpressionParser();
        FormatExpression fe = s.parse("tab_{ $0:'%6[\\\\[]s'}_name");
        FormatExpressionContext context = new SimpleFormatExpressionContext();
        context.setVariable("$0", 78);
        Assert.equals(fe.getValue(context), "tab_[[[[78_name");
    }

    @Test
    public void test15() {
        SimpleFormatExpressionParser s = new SimpleFormatExpressionParser();
        FormatExpression fe = s.parse("{'a\\\\a':'%6[\\\\[]s'}_name");
        FormatExpressionContext context = new SimpleFormatExpressionContext();
        context.setVariable("$0", 78);
        Assert.equals(fe.getValue(context), "[[[a\\a_name");
    }

}
