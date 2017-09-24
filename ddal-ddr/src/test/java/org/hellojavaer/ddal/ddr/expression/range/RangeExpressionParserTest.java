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
package org.hellojavaer.ddal.ddr.expression.range;

import org.hellojavaer.ddal.core.utils.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 29/11/2016.
 */
public class RangeExpressionParserTest {

    @Test
    public void test00() {
        List<String> expectedResult = new ArrayList<String>();
        expectedResult.add("");
        final List<String> result = new ArrayList<>();
        new RangeExpressionParser("").visit(new RangeExpressionItemVisitor() {

            @Override
            public void visit(Object val) {
                result.add((String) val);
            }
        });
        Assert.equals(result, expectedResult);

        expectedResult = new ArrayList<String>();
        expectedResult.add("0");
        final List<String> result1 = new ArrayList<>();
        new RangeExpressionParser("0").visit(new RangeExpressionItemVisitor() {

            @Override
            public void visit(Object val) {
                result1.add((String) val);
            }
        });
        Assert.equals(result1, expectedResult);
    }

    // 普通逗号拼接
    @Test
    public void test01() {
        List<String> expectedResult = new ArrayList<String>();
        expectedResult.add("");
        expectedResult.add("ab");
        expectedResult.add("");
        expectedResult.add("cd");
        expectedResult.add("");
        expectedResult.add("e f");
        expectedResult.add("\\hijk");
        expectedResult.add("");
        final List<String> result = new ArrayList<>();
        new RangeExpressionParser(",ab,,cd,  , e f , \\hijk ,").visit(new RangeExpressionItemVisitor() {

            @Override
            public void visit(Object val) {
                result.add((String) val);
            }
        });
        Assert.equals(result, expectedResult);
    }

    // 数字区间测试
    @Test
    public void test02() {
        List<Integer> expectedResult = new ArrayList<>();
        for (int i = -99; i <= 99; i++) {
            expectedResult.add(i);
        }
        final List<Integer> result0 = new ArrayList<>();
        new RangeExpressionParser("[-99..99]").visit(new RangeExpressionItemVisitor() {

            @Override
            public void visit(Object val) {
                result0.add((Integer) val);
            }
        });
        Assert.equals(result0, expectedResult);

        expectedResult = new ArrayList<Integer>();
        for (int i = 99; i >= -99; i--) {
            expectedResult.add(i);
        }
        final List<Integer> result1 = new ArrayList<>();
        new RangeExpressionParser("[99..-99]").visit(new RangeExpressionItemVisitor() {

            @Override
            public void visit(Object val) {
                result1.add((Integer) val);
            }
        });
        Assert.equals(result1, expectedResult);
    }

    // 字符区间测试
    @Test
    public void test03() {
        List<String> expectedResult = new ArrayList<String>();
        expectedResult.add("a");
        expectedResult.add("b");
        expectedResult.add("cd");
        expectedResult.add("ef");

        final List<String> result = new ArrayList<>();
        new RangeExpressionParser("['a',\"b\",\"cd\",'ef']").visit(new RangeExpressionItemVisitor() {

            @Override
            public void visit(Object val) {
                result.add((String) val);
            }
        });
        Assert.equals(result, expectedResult);

        //
        expectedResult = new ArrayList<String>();
        for (int i = 'a'; i <= 'z'; i++) {
            expectedResult.add(String.valueOf((char) i));
        }
        final List<String> result0 = new ArrayList<>();
        new RangeExpressionParser("['a'..'z']").visit(new RangeExpressionItemVisitor() {

            @Override
            public void visit(Object val) {
                result0.add((String) val);
            }
        });
        Assert.equals(result0, expectedResult);

        //
        expectedResult = new ArrayList<String>();
        for (int i = 'z'; i >= 'a'; i--) {
            expectedResult.add(String.valueOf((char) i));
        }
        final List<String> result1 = new ArrayList<>();
        new RangeExpressionParser("[\"z\"..'a']").visit(new RangeExpressionItemVisitor() {

            @Override
            public void visit(Object val) {
                result1.add((String) val);
            }
        });
        Assert.equals(result1, expectedResult);

        //
        expectedResult = new ArrayList<String>();
        for (int i = 'A'; i <= 'Z'; i++) {
            expectedResult.add(String.valueOf((char) i));
        }
        final List<String> result2 = new ArrayList<>();
        new RangeExpressionParser("['A'..\"Z\"]").visit(new RangeExpressionItemVisitor() {

            @Override
            public void visit(Object val) {
                result2.add((String) val);
            }
        });
        Assert.equals(result2, expectedResult);

        expectedResult = new ArrayList<String>();
        for (int i = 'Z'; i >= 'A'; i--) {
            expectedResult.add(String.valueOf((char) i));
        }
        final List<String> result3 = new ArrayList<>();
        new RangeExpressionParser("[\"Z\"..\"A\"]").visit(new RangeExpressionItemVisitor() {

            @Override
            public void visit(Object val) {
                result3.add((String) val);
            }
        });
        Assert.equals(result3, expectedResult);
    }

    // double 测试
    @Test
    public void test04() {
        List<Object> expectedResult = new ArrayList<>();
        expectedResult.add(1.);
        expectedResult.add(2.3);
        expectedResult.add(4);

        final List<Object> result = new ArrayList<>();
        new RangeExpressionParser("[1.,2.3,4]").visit(new RangeExpressionItemVisitor() {

            @Override
            public void visit(Object val) {
                result.add(val);
            }
        });
        Assert.equals(result, expectedResult);
    }

    // 区间内组合测试
    @Test
    public void test05() {
        List<Object> expectedResult = new ArrayList<>();
        expectedResult.add("\'");
        expectedResult.add("\"");
        expectedResult.add(1);
        expectedResult.add(2);
        expectedResult.add("3");
        expectedResult.add("a");
        expectedResult.add("b");
        expectedResult.add("c");
        expectedResult.add("d");
        expectedResult.add("ef\\");
        expectedResult.add("gh");
        final List<Object> result = new ArrayList<>();
        new RangeExpressionParser("['\\'',\"\\\"\",1..2,'3','a'..'b',\"c\"..\"d\",'\\ef\\\\',\"\\gh\"]").visit(new RangeExpressionItemVisitor() {

            @Override
            public void visit(Object val) {
                result.add(val);
            }
        });
        Assert.equals(result, expectedResult);
    }

    // 区间外组合测试
    @Test
    public void test06() {
        List<String> expectedResult = new ArrayList<String>();
        expectedResult.add("13");
        expectedResult.add("14");
        expectedResult.add("19");
        expectedResult.add("23");
        expectedResult.add("24");
        expectedResult.add("29");
        final List<String> result = new ArrayList<>();
        new RangeExpressionParser("[1..2][3..4,9]").visit(new RangeExpressionItemVisitor() {

            @Override
            public void visit(Object val) {
                result.add((String) val);
                // System.out.println(val);
            }
        });
        Assert.equals(result, expectedResult);
    }

    @Test
    public void test07() {
        List<String> expectedResult = new ArrayList<String>();
        expectedResult.add("a13");
        expectedResult.add("a14");
        expectedResult.add("a19");
        expectedResult.add("a23");
        expectedResult.add("a24");
        expectedResult.add("a29");
        final List<String> result = new ArrayList<>();
        new RangeExpressionParser("a[1..2][3..4,9]").visit(new RangeExpressionItemVisitor() {

            @Override
            public void visit(Object val) {
                result.add((String) val);
                // System.out.println(val);
            }
        });
        Assert.equals(result, expectedResult);
    }

    @Test
    public void test08() {
        List<String> expectedResult = new ArrayList<String>();
        expectedResult.add("13c");
        expectedResult.add("14c");
        expectedResult.add("19c");
        expectedResult.add("23c");
        expectedResult.add("24c");
        expectedResult.add("29c");
        final List<String> result = new ArrayList<>();
        new RangeExpressionParser("[1..2][3..4,9]c").visit(new RangeExpressionItemVisitor() {

            @Override
            public void visit(Object val) {
                result.add((String) val);
                // System.out.println(val);
            }
        });
        Assert.equals(result, expectedResult);
    }

    @Test
    public void test09() {
        List<String> expectedResult = new ArrayList<String>();
        expectedResult.add("1b3");
        expectedResult.add("1b4");
        expectedResult.add("1b9");
        expectedResult.add("2b3");
        expectedResult.add("2b4");
        expectedResult.add("2b9");
        final List<String> result = new ArrayList<>();
        new RangeExpressionParser("[1..2]b[3..4,9]").visit(new RangeExpressionItemVisitor() {

            @Override
            public void visit(Object val) {
                result.add((String) val);
                // System.out.println(val);
            }
        });
        Assert.equals(result, expectedResult);
    }

    @Test
    public void test10() {
        List<String> expectedResult = new ArrayList<String>();
        expectedResult.add("a1b3e");
        expectedResult.add("a1b4e");
        expectedResult.add("a1bce");
        expectedResult.add("a1bde");
        expectedResult.add("a2b3e");
        expectedResult.add("a2b4e");
        expectedResult.add("a2bce");
        expectedResult.add("a2bde");
        final List<String> result = new ArrayList<>();
        new RangeExpressionParser("a[1..2]b[3..4,'c'..'d']e").visit(new RangeExpressionItemVisitor() {

            @Override
            public void visit(Object val) {
                result.add((String) val);
                // System.out.println(val);
            }
        });
        Assert.equals(result, expectedResult);
    }

    @Test
    public void errorTest00() {
        try {
            final List<String> result = new ArrayList<>();
            new RangeExpressionParser("[\\\\,\\[\\],\\],\\,,\\.,\\s]").visit(new RangeExpressionItemVisitor() {

                @Override
                public void visit(Object val) {
                    result.add((String) val);
                }
            });
            throw new Error();
        } catch (RuntimeException e) {
        }
    }

    @Test
    public void errorTest02() {
        new RangeExpressionParser("[]").visit(new RangeExpressionItemVisitor() {

            @Override
            public void visit(Object val) {
                throw new Error();
            }
        });

        new RangeExpressionParser("ab[]").visit(new RangeExpressionItemVisitor() {

            @Override
            public void visit(Object val) {
                throw new Error();
            }
        });

        new RangeExpressionParser("[]cd").visit(new RangeExpressionItemVisitor() {

            @Override
            public void visit(Object val) {
                throw new Error();
            }
        });

        new RangeExpressionParser("ab[]cd").visit(new RangeExpressionItemVisitor() {

            @Override
            public void visit(Object val) {
                throw new Error();
            }
        });

        new RangeExpressionParser("[1..2]ab[]cd[3..4]").visit(new RangeExpressionItemVisitor() {

            @Override
            public void visit(Object val) {
                throw new Error();
            }
        });
    }

    @Test
    public void errorTest03() {
        try {
            new RangeExpressionParser("t0[1..2").visit(new RangeExpressionItemVisitor() {

                @Override
                public void visit(Object val) {
                    throw new Error();
                }
            });
            throw new Error();
        } catch (RangeExpressionException e) {
        }
    }

    @Test
    public void errorTest04() {
        try {
            new RangeExpressionParser("t0[0..]").visit(new RangeExpressionItemVisitor() {

                @Override
                public void visit(Object val) {
                    throw new Error();
                }
            });
            throw new Error();
        } catch (RangeExpressionException e) {

        }
    }

    @Test
    public void errorTest05() {
        try {
            new RangeExpressionParser("t00..9[").visit(new RangeExpressionItemVisitor() {

                @Override
                public void visit(Object val) {
                    throw new Error();
                }
            });
            throw new Error();
        } catch (RangeExpressionException e) {
        }
    }

    @Test
    public void errorTest06() {
        try {
            new RangeExpressionParser("t00..9]").visit(new RangeExpressionItemVisitor() {

                @Override
                public void visit(Object val) {
                    throw new Error();
                }
            });
            throw new Error();
        } catch (RangeExpressionException e) {
        }
    }

    @Test
    public void errorTest07() {
        try {
            new RangeExpressionParser("table_name_0[1..2..]").visit(new RangeExpressionItemVisitor() {

                @Override
                public void visit(Object val) {
                    throw new Error();
                }
            });
            throw new Error();
        } catch (RangeExpressionException e) {

        }
    }

    @Test
    public void errorTest08() {
        try {
            new RangeExpressionParser("t0[..1]").visit(new RangeExpressionItemVisitor() {

                @Override
                public void visit(Object val) {
                    throw new Error();
                }
            });
            throw new Error();
        } catch (RangeExpressionException e) {
        }
    }

}
