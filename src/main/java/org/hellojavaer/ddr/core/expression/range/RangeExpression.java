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
        int startIndex = 0;
        while (startIndex < str.length()) {
            startIndex = parse(str, null, startIndex, itemVisitor);
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
                                                       "expect a character('\\', '[', '], ',') to be escaped");
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
                    }
                    escape = true;
                } else if (ch == '[') {// 区间开始符号
                    String rangPrefix = null;
                    if (sb != null) {
                        rangPrefix = sb.toString();
                    } else {
                        rangPrefix = str.substring(startIndex, i);
                    }
                    // 读取开始值
                    int j = i + 1;
                    int startNum = 0;
                    int endNum = 0;
                    for (;; j++) {
                        if (j >= str.length()) {
                            throw new RangeExpressionException(str, j, (char) 0, "expect closed expression. eg: [0~9]'");
                        }
                        char ch0 = str.charAt(j);
                        if (ch0 >= '0' && ch0 <= '9') {
                            continue;
                        } else if (ch0 == '~') {// 区间分段符
                            if (i + 1 == j) {
                                throw new RangeExpressionException(str, j, ch0, "expect closed expression. eg: [0~9]");
                            } else {
                                String startStr = str.substring(i + 1, j);
                                startNum = Integer.valueOf(startStr);
                                break;
                            }
                        } else {
                            throw new RangeExpressionException(str, j, ch0, "expect closed expression. eg: [0~9]");
                        }
                    }
                    // 读取结束值
                    j++;
                    i = j;
                    for (;; j++) {
                        if (j >= str.length()) {
                            throw new RangeExpressionException(str, j, (char) 0, ']');
                        }
                        char ch0 = str.charAt(j);
                        if (ch0 >= '0' && ch0 <= '9') {
                            continue;
                        } else if (ch0 == ']') {// 区间结束符号
                            if (i == j) {
                                throw new RangeExpressionException(str, j, ch0, "expect closed expression. eg: [0~9]");
                            } else {
                                String endStr = str.substring(i, j);
                                endNum = Integer.valueOf(endStr);
                                break;
                            }
                        } else {
                            throw new RangeExpressionException(str, j, ch0, ']');
                        }
                    }
                    int nextStart = ++j;
                    i = j;
                    int e = 0;
                    if (startNum <= endNum) {// 升区间
                        for (int k = startNum; k <= endNum; k++) {
                            e = parse(str, rangPrefix + k, nextStart, itemVisitor);
                        }
                        return e;
                    } else {// 降区间
                        for (int k = startNum; k >= endNum; k--) {
                            e = parse(str, rangPrefix + k, nextStart, itemVisitor);
                        }
                        return e;
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
