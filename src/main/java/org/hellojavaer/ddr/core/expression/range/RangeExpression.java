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
        boolean escape = false;
        int nextStart = 0;
        int level = 0;// 语义深度
        for (int i = 0; i < str.length(); i++) {
            char ch = str.charAt(i);
            if (escape) {
                if (ch == '\\' || ch == '[' || ch == ']' || ch == '~' || ch == ',') {
                    escape = false;
                } else {
                    throw new RangeExpressionException(str, i, ch, '\\', '[', ']', '~', ',');
                }
            } else {
                if (ch == '\\') {
                    escape = true;
                } else if (ch == ',') {// 优化
                    if (level == 0) {
                        parse(str, null, nextStart, i, itemVisitor);
                        nextStart = i + 1;
                    }
                } else if (ch == '[') {
                    level++;
                } else if (ch == ']') {
                    level--;
                }
            }
        }
        parse(str, null, nextStart, str.length(), itemVisitor);
    }

    private static void parse(String str, String prefix, int startIndex, int endIndex, RangeItemVisitor itemVisitor) {
        boolean escape = false;
        StringBuilder sb = null;
        if (prefix != null) {
            sb = new StringBuilder();
            sb.append(prefix);
        }
        for (int i = startIndex;; i++) {
            if (i > endIndex) {
                return;
            } else if (i == endIndex) {
                if (sb != null) {
                    itemVisitor.visit(sb.toString());
                } else {
                    itemVisitor.visit(str.substring(startIndex, endIndex));
                }
                return;
            }
            char ch = str.charAt(i);
            if (ch == ',') {
                if (sb != null) {
                    itemVisitor.visit(sb.toString());
                    sb = null;
                } else {
                    itemVisitor.visit(str.substring(startIndex, i));
                }
            } else if (escape) {// 处理转义
                if (ch == '[' || ch == ']' || ch == '~' || ch == ',' || ch == '\\') {//
                    sb.append(ch);
                    escape = false;
                } else {
                    throw new RangeExpressionException(str, i, ch, '\\', '[', ']', '~', ',');
                }
            } else {//
                if (ch == '\\') {// 转义后使用sb做缓存,否则直接截取str子串
                    if (sb == null) {
                        sb = new StringBuilder();
                    }
                    escape = true;
                } else if (ch == '[') {// 区间开始符号
                    String rangPrefix = "";
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
                        if (j >= endIndex) {
                            throw new RangeExpressionException(str, j, ch, "'0,1,2..9'");
                        }
                        char ch0 = str.charAt(j);
                        if (ch0 >= '0' && ch0 <= '9') {
                            continue;
                        } else if (ch0 == '~') {// 区间分段符
                            String startStr = str.substring(i + 1, j);
                            startNum = Integer.valueOf(startStr);
                            break;
                        } else {
                            throw new RangeExpressionException(str, j, ch, '~');
                        }
                    }
                    // 读取结束值
                    j++;
                    i = j;
                    for (;; j++) {
                        if (j >= endIndex) {
                            throw new RangeExpressionException(str, j, ch, "'0,1,2..9'");
                        }
                        char ch0 = str.charAt(j);
                        if (ch0 >= '0' && ch0 <= '9') {
                            continue;
                        } else if (ch0 == ']') {// 区间结束符号
                            String endStr = str.substring(i, j);
                            endNum = Integer.valueOf(endStr);
                            break;
                        } else {
                            throw new RangeExpressionException(str, j, ch, ']');
                        }
                    }
                    int nextStart = ++j;
                    i = j;
                    if (startNum <= endNum) {// 升区间
                        for (int k = startNum; k <= endNum; k++) {
                            parse(str, rangPrefix + k, nextStart, endIndex, itemVisitor);
                        }
                        return;
                    } else {// 降区间
                        for (int k = startNum; k >= endNum; k--) {
                            parse(str, rangPrefix + k, nextStart, endIndex, itemVisitor);
                        }
                        return;
                    }
                } else {// 普通字符
                    if (sb != null) {
                        sb.append(ch);
                    } else {
                        continue;
                    }
                }
            }
        }
    }

}
