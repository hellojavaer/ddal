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
package org.hellojavaer.ddr.core.expression.range;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">zoukaiming[邹凯明]</a>,created on 24/11/2016.
 */
public class RangeExpression {

    public static void parse(String str, RangeItemVisitor itemVisitor) {
        for (int startIndex = 0; startIndex <= str.length();) {
            startIndex = parse(str, null, startIndex, itemVisitor);
            startIndex++;
        }
    }

    private static int parse(String str, String prefix, int startIndex, RangeItemVisitor itemVisitor) {
        boolean escape = false;
        StringBuilder sb = null;
        if (prefix != null) {
            sb = new StringBuilder();
            sb.append(prefix);
        }
        for (int i = startIndex;; i++) {
            if (i >= str.length()) {
                if (escape) {
                    throw new RangeExpressionException(str, str.length(), (char) 0,
                                                       "Out of statement block, escape character '\\' can only be used for character '\\', ',' , '[' and ']' ");
                }
                if (sb != null) {
                    itemVisitor.visit(sb.toString());
                } else {
                    itemVisitor.visit(str.substring(startIndex, i));
                }
                return i;
            }
            char ch = str.charAt(i);
            if (escape) {// 语句块外特殊字符
                if (ch == '[' || ch == ']' || ch == ',' || ch == '\\') {//
                    sb.append(ch);
                    escape = false;
                } else {
                    throw new RangeExpressionException(str, i, ch,
                                                       "Out of statement block, escape character '\\' can only be used for character '\\', ',' , '[' and ']' ");
                }
            } else {//
                if (ch == ',') {// 递归终结符
                    if (sb != null) {
                        itemVisitor.visit(sb.toString());
                    } else {
                        itemVisitor.visit(str.substring(startIndex, i));
                    }
                    return i;
                } else if (ch == '\\') {// 转义后使用sb做缓存,否则直接截取str子串
                    if (sb == null) {
                        sb = new StringBuilder();
                        sb.append(str.substring(startIndex, i));
                    }
                    escape = true;
                } else if (ch == '[') {// 区间开始符号 \\ 特殊字符 , [ ] \ ~ \s
                    // 获取区域结束符位置
                    int nextStart = -1;
                    boolean escape0 = false;
                    for (int k = i + 1; k < str.length(); k++) {
                        char ch0 = str.charAt(k);
                        if (escape0) {
                            escape0 = false;
                        } else if (ch0 == '\\') {
                            escape0 = true;
                        } else if (ch0 == ']') {
                            nextStart = k + 1;
                            break;
                        }
                    }
                    if (nextStart == -1) {
                        throw new RangeExpressionException(str, str.length(), (char) 0,
                                                           "Expect closed expression. eg: [0,0~99,a-z,A-Z]'");
                    }
                    // 获取前缀
                    String rangPrefix = null;
                    if (sb != null) {
                        rangPrefix = sb.toString();
                    } else {
                        rangPrefix = str.substring(startIndex, i);
                    }
                    // 读取开始值
                    int j = i + 1;
                    //
                    int erang = i + 1;
                    //
                    int status0 = 0;// 0:初始化,1:数字,2:小写,3:多个小写,4:大写,5:多个大小,7:混合
                    int status1 = 0;//
                    int status_temp = 0;
                    boolean range = false;
                    boolean escape1 = false;
                    Object rangStart = null;
                    StringBuilder sb1 = null;
                    int xpos = 0;// ~ 位置
                    for (;; j++) {
                        if (j >= str.length()) {
                            throw new RangeExpressionException(str, j, (char) 0,
                                                               "Expect closed expression. eg: [0,0~99,a-z,A-Z]'");
                        }
                        char ch1 = str.charAt(j);
                        if (escape1) {
                            if (ch1 == '\\' || ch1 == '[' || ch1 == ']' || ch1 == ',') {
                                sb1.append(ch1);
                            } else if (ch1 == 's') {
                                sb1.append(' ');
                            } else {
                                throw new RangeExpressionException(str, i, ch,
                                                                   "in statement block, escape character '\\' can only be used for character '\\', ',' , '[', ']' and 's' ");
                            }
                            escape1 = false;
                            continue;
                        }
                        if (range) {
                            status_temp = status1;
                        } else {
                            status_temp = status0;
                        }
                        if (ch1 == '\\') {// 转义
                            sb1 = new StringBuilder();
                            sb1.append(str.substring(erang + 1, j));
                            escape1 = true;
                        } else if (ch1 >= '0' && ch1 <= '9') {// 数字
                            if (status_temp == 0) {
                                status_temp = 1;
                            } else if (status_temp != 1) {
                                status_temp = 6;
                            }
                        } else if (ch1 >= 'a' && ch1 <= 'z') {// 小写字母
                            if (status_temp == 0) {
                                status_temp = 2;
                            } else if (status_temp == 2) {
                                status_temp = 3;
                            } else {
                                status_temp = 6;
                            }
                        } else if (ch1 >= 'A' && ch1 <= 'Z') {// 大写字母
                            if (status_temp == 0) {
                                status_temp = 4;
                            } else if (status_temp == 4) {
                                status_temp = 5;
                            } else {
                                status_temp = 6;
                            }
                        } else if (ch1 == '~') {// support 1,2,4
                            if (status_temp == 1) {
                                rangStart = Integer.parseInt(str.substring(erang, j));
                            } else if (status_temp == 2 || status_temp == 4) {
                                rangStart = str.charAt(j - 1);
                            } else {// ~
                                throw new RangeExpressionException(str, j, ch1,
                                                                   "expression '~' only support [0~99] ,[a~z] or[A~Z]");
                            }
                            xpos = j;
                            range = true;
                            continue;// //
                        } else if (ch1 == ',' || ch1 == ']') {// 结束符
                            range = false;
                            int epos = 0;// 返回下一个开始位置
                            if (status1 != 0) {// ~
                                if (status0 != status1) {
                                    throw new RangeExpressionException(str, j, ch1,
                                                                       "start expression and end expression not match. eg: [089,0~99,a-z,A-Z]'");
                                } else {// 区间表达式
                                    if (status1 == 1) {// 数字
                                        int s = ((Integer) rangStart).intValue();
                                        int e = Integer.parseInt(str.substring(xpos + 1, j));
                                        if (s <= e) {
                                            for (int k = s; k <= e; k++) {
                                                epos = parse(str, rangPrefix + k, nextStart, itemVisitor);
                                            }
                                        } else {
                                            for (int k = s; k >= e; k--) {
                                                epos = parse(str, rangPrefix + k, nextStart, itemVisitor);
                                            }
                                        }
                                    } else {// 大小或小写单字母
                                        char s = ((Character) rangStart).charValue();
                                        char e = str.charAt(j - 1);
                                        if (s <= e) {
                                            for (int k = s; k <= e; k++) {
                                                epos = parse(str, rangPrefix + (char) k, nextStart, itemVisitor);
                                            }
                                        } else {
                                            for (int k = s; k >= e; k--) {
                                                epos = parse(str, rangPrefix + (char) k, nextStart, itemVisitor);
                                            }
                                        }
                                    }
                                }
                            } else {// 单个表达式,支持特殊字符
                                String prefix1 = null;
                                if (sb1 != null) {
                                    prefix1 = rangPrefix + sb1.toString();
                                } else {
                                    prefix1 = rangPrefix + str.substring(erang, j);
                                }
                                epos = parse(str, prefix1, nextStart, itemVisitor);
                            }
                            // 重置标识位
                            status_temp = 0;
                            status0 = 0;
                            status1 = 0;
                            if (ch1 == ']') {// return
                                return epos;
                            } else {
                                erang = j + 1;
                            }
                        } else {
                            status0 = 6;
                            if (sb1 != null) {
                                sb1.append(ch1);
                            }
                        }
                        if (range) {
                            status1 = status_temp;
                        } else {
                            status0 = status_temp;
                        }
                    }
                } else if (ch == ']') {
                    throw new RangeExpressionException(str, i, ch, "expect closed expression. eg: [0~9]");
                } else {// 普通字符
                    if (sb != null) {
                        sb.append(ch);
                    } else {
                        continue;
                    }
                }
            }
        }// for
    }
}
