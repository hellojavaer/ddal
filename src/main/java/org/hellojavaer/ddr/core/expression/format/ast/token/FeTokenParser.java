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
package org.hellojavaer.ddr.core.expression.format.ast.token;

import org.hellojavaer.ddr.core.text.StringFormat;

/**
 *
 *
 * @author <a href="mailto:hellojavaer@gmail.com">zoukaiming[邹凯明]</a>,created on 17/11/2016.
 */
public class FeTokenParser {

    private String             str;
    private int                index;
    private int                stat          = 0;

    private static final int[] TAGS          = new int[256];
    private static int         IS_DIGIST     = 1;
    private static int         IS_VAR_HEADER = 2;
    private static int         IS_VAR_BODY   = 4;
    private static int         IS_ALPHA      = 8;

    static {
        for (int i = '0'; i <= '9'; i++) {
            TAGS[i] |= IS_DIGIST | IS_VAR_BODY;
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

    public FeToken next() {
        if (index >= str.length()) {
            return null;
        }
        eatSpace();
        int s = index;
        char ch = str.charAt(index);
        if (stat == 0) {// 纯文本
            if (ch == '{') {
                stat = 1;
                FeToken t = new FeToken(FeTokenType.LCURLY, null, s, index);
                index++;
                return t;
            } else {
                for (; index < str.length(); index++) {
                    ch = str.charAt(index);
                    if (ch == '{') {
                        break;
                    } else if (ch == '}') {
                        throw new IllegalStateException("unexpected '}' at index " + index);
                    } else {
                        continue;
                    }
                }
                return new FeToken(FeTokenType.PLAIN_TEXT, str.substring(s, index), s, index - 1);
            }
        } else if (stat == 1) {// 语句块
            ch = str.charAt(index);
            if ((TAGS[ch] & 2) != 0) {// 变量
                for (index++; index < str.length() && (TAGS[str.charAt(index)] & 4) != 0; index++) {
                }
                return new FeToken(FeTokenType.VAR, str.substring(s, index), s, index - 1);
            } else if ((TAGS[ch] & 1) != 0) {// 数字
                for (index++; index < str.length() && (TAGS[str.charAt(index)] & 1) != 0; index++) {
                }
                return new FeToken(FeTokenType.NUMBER, str.substring(s, index), s, index - 1);
            } else if (ch == '\'' || ch == '\"') {// 字符
                StringBuilder sb = new StringBuilder();
                boolean escape = false;
                for (index++; index < str.length() && (str.charAt(index) != ch); index++) {
                    char c0 = str.charAt(index);
                    if (escape) {
                        if (c0 == '\\' || c0 == ch) {
                            sb.append(c0);
                        } else {
                            sb.append('\\');
                            sb.append(c0);
                        }
                        escape = false;
                    } else {
                        if (c0 == '\\') {
                            escape = true;
                            continue;
                        } else {
                            sb.append(c0);
                        }
                    }
                }
                return new FeToken(FeTokenType.STRING, sb.toString(), s, index - 1);
            } else if (ch == ':') {
                stat = 2;
                FeToken t = new FeToken(FeTokenType.COLON, null, s, index - 1);
                index++;
                return t;
            } else if (ch == '}') {// stat -> 0
                stat = 0;
                FeToken t = new FeToken(FeTokenType.RCURLY, null, s, index - 1);
                index++;
                return t;
            } else {
                throw new IllegalStateException("unexpected token '" + ch + "' at index " + index + " for string '"
                                                + str + "'");
            }
        } else {// stat = 2
            stat = 1;
            for (; index < str.length() && (TAGS[str.charAt(index)] & 8) == 0; index++) {
            }
            index++;
            String pattern = str.substring(s, index);
            StringFormat sf = new StringFormat(pattern);
            return new FeToken(FeTokenType.FORMAT_PATTERN, sf, s, index - 1);
        }
    }

    private void eatSpace() {
        for (; index < str.length() && str.charAt(index) == ' '; index++) {
        }
    }

}
