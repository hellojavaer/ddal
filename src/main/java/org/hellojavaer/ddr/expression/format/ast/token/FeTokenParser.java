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
package org.hellojavaer.ddr.expression.format.ast.token;

import org.hellojavaer.ddr.expression.format.exception.FormatExpressionException;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 17/11/2016.
 */
public class FeTokenParser {

    private String             str;
    private int                index         = 0;           // 指向下次调用的开始位置
    private int                stat          = 0;

    private static final int[] TAGS          = new int[256];
    private static int         IS_DIGIT      = 1;
    private static int         IS_VAR_HEADER = 2;
    private static int         IS_VAR_BODY   = 4;
    private static int         IS_ALPHA      = 8;

    static {
        for (int i = '0'; i <= '9'; i++) {
            TAGS[i] |= IS_DIGIT | IS_VAR_BODY;
        }
        for (int i = 'a'; i <= 'z'; i++) {
            TAGS[i] |= IS_VAR_HEADER | IS_VAR_BODY | IS_ALPHA;
        }
        for (int i = 'A'; i <= 'Z'; i++) {
            TAGS[i] |= IS_VAR_HEADER | IS_VAR_BODY | IS_ALPHA;
        }
        TAGS['$'] |= IS_VAR_HEADER | IS_VAR_BODY;
        TAGS['_'] |= IS_VAR_HEADER | IS_VAR_BODY;
    }

    public FeTokenParser(String str) {
        this.str = str;
    }

    /**
     * 
     * @return
     */
    public FeToken next() {
        if (index >= str.length()) {
            return new FeToken(FeTokenType.NULL, null, str.length(), str.length());
        }
        int s = index;// s:开始标记
        if (stat == 0) {// 纯文本
            char ch = str.charAt(index);
            // 1.判断特殊字符
            if (ch == '{') {
                stat = 1;// 改变状态
                index++;
                return new FeToken(FeTokenType.LCURLY, null, s, s + 1);
            }
            // 2.普通字符
            boolean escape = false;
            StringBuilder sb = null;
            for (;; index++) {// 终结符:NULL
                if (index >= str.length()) {
                    if (escape) {
                        throw new FormatExpressionException("illegal escape character '\\' at index " + (index - 1)
                                                            + ". Source string is '" + str + "'");
                    }
                    break;
                }
                ch = str.charAt(index);
                if (escape) {
                    if (ch == '\\' || ch == '{' || ch == '}') {
                        sb.append(ch);
                    } else {
                        throw new FormatExpressionException("Character '" + ch + "' can't be escaped at index " + index
                                                            + ". Source string is '" + str + "'");
                    }
                    escape = false;
                } else {
                    if (ch == '\\') {
                        escape = true;
                        if (sb == null) {
                            sb = new StringBuilder();
                            sb.append(str.substring(s, index));
                        }
                    } else if (ch == '{') {// 终结符:NULL
                        break;
                    } else if (ch == '}') {
                        throw new FormatExpressionException("Unexpected character '}' at index " + index
                                                            + ". Source string is '" + str + "'");
                    } else {
                        if (sb != null) {
                            sb.append(ch);
                        }
                    }
                }
            }
            String data = null;
            if (sb != null) {
                data = sb.toString();
            } else {
                data = str.substring(s, index);
            }
            return new FeToken(FeTokenType.PLAIN_TEXT, data, s, index);
        } else {// 语句块
            for (; str.charAt(index) == ' '; index++) {// eat space
                if (index >= str.length()) {
                    throw new FormatExpressionException(
                                                        "Not closed expression, expect character '}' at the end of string '"
                                                                + str + "'");
                }
            }
            s = index;// s:开始标记
            char ch = str.charAt(index);
            if ((TAGS[ch] & 2) != 0) {// 变量
                for (index++; index < str.length() && (TAGS[str.charAt(index)] & 4) != 0; index++) {
                }
                return new FeToken(FeTokenType.VAR, str.substring(s, index), s, index - 1);
            } else if ((TAGS[ch] & 1) != 0) {// 数字
                for (index++; index < str.length() && (TAGS[str.charAt(index)] & 1) != 0; index++) {
                }
                return new FeToken(FeTokenType.NUMBER, str.substring(s, index), s, index - 1);
            } else if (ch == '\'' || ch == '\"') {// 字符
                StringBuilder sb = null;
                boolean escape = false;
                for (index++;; index++) {
                    if (index >= str.length()) {
                        throw new FormatExpressionException(
                                                            "Not closed expression. Expect character '}' at the end of string "
                                                                    + str);
                    }
                    char c0 = str.charAt(index);
                    if (escape) {
                        if (c0 == '\\' || c0 == '\'' || c0 == '\"') {
                            sb.append(c0);
                        } else {
                            throw new FormatExpressionException("Character character '" + c0
                                                                + "' can't be escaped at index " + index
                                                                + ". Source string is '" + str + "'");
                        }
                        escape = false;
                    } else {
                        if (c0 == '\\') {
                            escape = true;
                            sb = new StringBuilder();
                            sb.append(str.substring(s + 1, index));
                            continue;
                        } else if (c0 == ch) {// 终结符:
                            break;
                        } else {
                            if (sb != null) {
                                sb.append(c0);
                            }
                        }
                    }
                }
                String data = null;
                if (sb != null) {
                    data = sb.toString();
                } else {
                    data = str.substring(s + 1, index);
                }
                FeToken t = new FeToken(FeTokenType.STRING, data, s, index + 1);
                index++;
                return t;
            } else if (ch == ':') {
                FeToken t = new FeToken(FeTokenType.COLON, null, s, s + 1);
                index++;
                return t;
            } else if (ch == '}') {// stat -> 0
                stat = 0;
                FeToken t = new FeToken(FeTokenType.RCURLY, null, s, s + 1);
                index++;
                return t;
            } else {
                throw new FormatExpressionException("Unexpected character '" + ch + "' at index " + index
                                                    + ". Source string is '" + str + "'");
            }
        }
    }

}
