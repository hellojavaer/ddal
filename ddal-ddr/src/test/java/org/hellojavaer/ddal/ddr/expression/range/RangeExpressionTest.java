package org.hellojavaer.ddal.ddr.expression.range;

import org.hellojavaer.ddal.ddr.utils.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 29/11/2016.
 */
public class RangeExpressionTest {

    @Test
    public void test01() {
        List<String> expectedResult = new ArrayList<String>();
        expectedResult.add("ab");
        expectedResult.add("");
        expectedResult.add("cd");
        expectedResult.add("\\[],");
        expectedResult.add("");
        final List<String> result = new ArrayList<>();
        RangeExpression.parse("ab,,cd,\\\\\\[\\]\\,,", new RangeItemVisitor() {

            @Override
            public void visit(String val) {
                result.add(val);
                // System.out.println(val);
            }
        });
        Assert.equals(result, expectedResult);
    }

    @Test
    public void test02() {
        List<String> expectedResult = new ArrayList<String>();
        for (int i = 'a'; i <= 'z'; i++) {
            expectedResult.add(String.valueOf((char) i));
        }
        final List<String> result = new ArrayList<>();
        RangeExpression.parse("[a~z]", new RangeItemVisitor() {

            @Override
            public void visit(String val) {
                result.add(val);
            }
        });
        Assert.equals(result, expectedResult);
    }

    @Test
    public void test03() {
        List<String> expectedResult = new ArrayList<String>();
        for (int i = 'z'; i >= 'a'; i--) {
            expectedResult.add(String.valueOf((char) i));
        }
        final List<String> result = new ArrayList<>();
        RangeExpression.parse("[z~a]", new RangeItemVisitor() {

            @Override
            public void visit(String val) {
                result.add(val);
            }
        });
        Assert.equals(result, expectedResult);
    }

    @Test
    public void test004() {
        List<String> expectedResult = new ArrayList<String>();
        for (int i = 'A'; i <= 'Z'; i++) {
            expectedResult.add(String.valueOf((char) i));
        }
        final List<String> result = new ArrayList<>();
        RangeExpression.parse("[A~Z]", new RangeItemVisitor() {

            @Override
            public void visit(String val) {
                result.add(val);
            }
        });
        Assert.equals(result, expectedResult);
    }

    @Test
    public void test05() {
        List<String> expectedResult = new ArrayList<String>();
        for (int i = 'Z'; i >= 'A'; i--) {
            expectedResult.add(String.valueOf((char) i));
        }
        final List<String> result = new ArrayList<>();
        RangeExpression.parse("[Z~A]", new RangeItemVisitor() {

            @Override
            public void visit(String val) {
                result.add(val);
            }
        });
        Assert.equals(result, expectedResult);
    }

    @Test
    public void test06() {
        List<String> expectedResult = new ArrayList<String>();
        for (int i = -99; i <= 99; i++) {
            expectedResult.add(String.valueOf((int) i));
        }
        final List<String> result = new ArrayList<>();
        RangeExpression.parse("[-99~99]", new RangeItemVisitor() {

            @Override
            public void visit(String val) {
                result.add(val);
            }
        });
        Assert.equals(result, expectedResult);
    }

    @Test
    public void test07() {
        List<String> expectedResult = new ArrayList<String>();
        for (int i = 99; i >= -99; i--) {
            expectedResult.add(String.valueOf((int) i));
        }
        final List<String> result = new ArrayList<>();
        RangeExpression.parse("[99~-99]", new RangeItemVisitor() {

            @Override
            public void visit(String val) {
                result.add(val);
            }
        });
        Assert.equals(result, expectedResult);
    }

    @Test
    public void test08() {
        List<String> expectedResult = new ArrayList<String>();
        expectedResult.add("a1b3c");
        expectedResult.add("a1b4c");
        expectedResult.add("a1b9c");
        expectedResult.add("a2b3c");
        expectedResult.add("a2b4c");
        expectedResult.add("a2b9c");
        final List<String> result = new ArrayList<>();
        RangeExpression.parse("a[1~2]b[3~4,9]c", new RangeItemVisitor() {

            @Override
            public void visit(String val) {
                result.add(val);
                // System.out.println(val);
            }
        });
        Assert.equals(result, expectedResult);
    }

    @Test
    public void test09() {
        List<String> expectedResult = new ArrayList<String>();
        expectedResult.add("0");
        final List<String> result = new ArrayList<>();
        RangeExpression.parse("0", new RangeItemVisitor() {

            @Override
            public void visit(String val) {
                result.add(val);
                // System.out.println(val);
            }
        });
        Assert.equals(result, expectedResult);
    }

    @Test
    public void test10() {
        List<String> expectedResult = new ArrayList<String>();
        expectedResult.add("\\");
        expectedResult.add("[]");
        expectedResult.add("]");
        expectedResult.add(",");
        expectedResult.add("~");
        expectedResult.add(" ");
        final List<String> result = new ArrayList<>();
        RangeExpression.parse("[\\\\,\\[\\],\\],\\,,\\~,\\s]", new RangeItemVisitor() {

            @Override
            public void visit(String val) {
                result.add(val);
                //System.out.println(val);
            }
        });
        Assert.equals(result, expectedResult);
    }

    @Test
    public void parse000() {
        // RangeExpression.parse("hi !,I miss you so much. [1~3] day(s)", new RangeItemVisitor() {
        //
        // @Override
        // public void visit(String val) {
        // System.out.println(val);
        // }
        // });
    }

    /**
     * illegal escape
     */
    @Test
    public void errorTest01() {
        try {
            RangeExpression.parse("ab\\a", new RangeItemVisitor() {

                @Override
                public void visit(String val) {
                    //System.out.println(val);
                }
            });
            throw new Error();
        } catch (RangeExpressionException e) {
        }
    }

    /**
     * illegal escape
     */
    @Test
    public void errorTest02() {
        RangeExpression.parse("[]", new RangeItemVisitor() {

            @Override
            public void visit(String val) {
                //System.out.println(val);
            }
        });
    }

    @Test
    public void errorTest03() {
        try {
            RangeExpression.parse("t0[~]", new RangeItemVisitor() {

                @Override
                public void visit(String val) {
                    //System.out.println(val);
                }
            });
            throw new Error();
        } catch (RangeExpressionException e) {
        }
    }

    @Test
    public void errorTest04() {
        try {
            RangeExpression.parse("t0[0~]", new RangeItemVisitor() {

                @Override
                public void visit(String val) {
                    //System.out.println(val);
                }
            });
            throw new Error();
        } catch (RangeExpressionException e) {

        }
    }

    @Test
    public void errorTest05() {
        try {
            RangeExpression.parse("t00~9[", new RangeItemVisitor() {

                @Override
                public void visit(String val) {
                    //System.out.println(val);
                }
            });
            throw new Error();
        } catch (RangeExpressionException e) {

        }
    }

    @Test
    public void errorTest06() {
        try {
            RangeExpression.parse("t00~9]", new RangeItemVisitor() {

                @Override
                public void visit(String val) {
                    //System.out.println(val);
                }
            });
            throw new Error();
        } catch (RangeExpressionException e) {
        }
    }

    @Test
    public void errorTest07() {
        try {
            RangeExpression.parse("table_name_0[1~2~]", new RangeItemVisitor() {

                @Override
                public void visit(String val) {
                    //System.out.println(val);
                }
            });
            throw new Error();
        } catch (RangeExpressionException e) {

        }
    }

    @Test
    public void errorTest08() {
        try {
            RangeExpression.parse("t0[~1]", new RangeItemVisitor() {

                @Override
                public void visit(String val) {
                    //System.out.println(val);
                }
            });
            throw new Error();
        } catch (RangeExpressionException e) {
        }
    }

}
