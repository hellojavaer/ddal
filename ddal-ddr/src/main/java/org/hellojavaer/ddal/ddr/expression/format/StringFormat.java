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
package org.hellojavaer.ddal.ddr.expression.format;

/**
 * <pre>
 * for input 123
 *  %4s -> 0123
 *  %4S -> 0123
 *  %4[a]s -> a123
 *  %4[a]S -> a123
 *  %4[abc]s -> abc123
 *  %4[abc]S -> c123
 *
 * for input 123456
 *  %4s -> 123456
 *  %4S -> 3456
 *  %4[a]s -> 123456
 *  %4[a]S -> 3456
 *  %4[abc]s -> 123456
 *  %4[abc]S -> 3456
 *
 * for input 123
 *  %-4s -> 1230
 *  %-4S -> 1230
 *  %-4[a]s -> 123a
 *  %-4[a]S -> 123a
 *  %-4[abc]s -> 123abc
 *  %-4[abc]S -> 123a
 *
 * for input 123456
 *  %-4s -> 123456
 *  %-4S -> 1234
 *  %-4[a]s -> 123456
 *  %-4[a]S -> 1234
 *  %-4[abc]s -> 123456
 *  %-4[abc]S -> 1234
 *  </pre>
 *
 *  only '/' '[' ']' can be escaped in '[]'.
 * 
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 16/11/2016.
 */
public class StringFormat {

    private String  pattern;

    private String  repeatString = "0";
    private boolean append       = false;
    private boolean force        = false;
    private int     length       = 0;

    public StringFormat(String pattern) {
        this.pattern = pattern;
        init();
    }

    public String format(String str) {
        int x = length - str.length();
        if (!force) {
            if (length > 0) {
                int count = (x + repeatString.length() - 1) / repeatString.length();
                StringBuilder sb = new StringBuilder();
                if (append) {
                    sb.append(str);
                    for (int k = 0; k < count; k++) {
                        sb.append(repeatString);
                    }
                } else {
                    for (int k = 0; k < count; k++) {
                        sb.append(repeatString);
                    }
                    sb.append(str);
                }
                return sb.toString();
            } else {
                return str;
            }
        } else {
            if (x > 0) {// 补齐
                int count = (x + repeatString.length() - 1) / repeatString.length();
                StringBuilder sb = new StringBuilder();
                if (append) {
                    sb.append(str);
                    for (int k = 0; k < count; k++) {
                        sb.append(repeatString);
                    }
                    return sb.substring(0, length);
                } else {
                    for (int k = 0; k < count; k++) {
                        sb.append(repeatString);
                    }
                    sb.append(str);
                    return sb.substring(sb.length() - length, sb.length());
                }
            } else if (x < 0) {// 删减
                if (append) {
                    return str.substring(0, length);
                } else {
                    return str.substring(-x, str.length());
                }
            } else {
                return str;
            }
        }
    }

    public void init() {
        if (pattern.charAt(0) != '%') {
            throw new IllegalArgumentException("String pattern must be start with '%'. source pattern is '" + pattern
                                               + "'");
        }
        int index = 1;
        char ch = pattern.charAt(1);
        // 解析方向
        if (ch == '-') {
            append = true;
            index = 2;
        } else if (ch == '+') {
            append = false;
            index = 2;
        }
        // 解析长度
        for (int i = index;; i++) {
            if (i >= pattern.length()) {
                throw new IllegalArgumentException("Expect digit after '%' at index " + i + ". source pattern is '"
                                                   + pattern + "'");
            }
            ch = pattern.charAt(i);
            if (ch == 's' || ch == 'S' || ch == '[') {
                if (index == i) {
                    throw new IllegalArgumentException("Expect digit after '%' at index " + i + ". source pattern is '"
                                                       + pattern + "'");
                } else {
                    index = i;
                    break;
                }
            } else {
                if (ch >= '0' && ch <= '9') {
                    this.length = (ch - '0') + length * 10;
                } else {
                    throw new IllegalArgumentException("Expect digit after '%' at index " + i + ". source pattern is '"
                                                       + pattern + "'");
                }
            }
        }
        // 解析append值
        if (ch == '[') {
            index++;
            boolean escape = false;
            StringBuilder sb = null;// \[]
            for (int i = index;; i++) {
                if (i >= pattern.length()) {
                    throw new IllegalArgumentException("Expect character ']' at index " + i + ". source pattern is '"
                                                       + pattern + "'");// 未闭合
                }
                ch = pattern.charAt(i);
                if (escape) {// 处理转义
                    if (ch == '\\' || ch == '[' || ch == ']') {// key_word
                        sb.append(ch);
                    } else if (ch == 's') {
                        sb.append(' ');
                    } else {
                        throw new IllegalArgumentException(
                                                           "Character '"
                                                                   + ch
                                                                   + "',at index "
                                                                   + i
                                                                   + ", can't be escaped. Only '\\','[' and ']' can be escaped. source pattern is '"
                                                                   + pattern + "'");
                    }
                    escape = false;
                } else if (ch == '\\') {
                    if (sb == null) {
                        sb = new StringBuilder();
                        sb.append(pattern.substring(index, i));
                    }
                    escape = true;
                } else if (ch == '[') {
                    throw new IllegalArgumentException("Unexpected character '" + ch + "' at index " + i
                                                       + ". source pattern is '" + pattern + "'");
                } else if (ch == ']') {// 出口
                    if (sb != null) {
                        repeatString = sb.toString();
                    } else {
                        repeatString = pattern.substring(index, i);
                    }
                    if (i == index) {
                        throw new IllegalArgumentException("Expect at last one character between '[]' at index "
                                                           + index + ". source pattern is '" + pattern + "'");
                    }
                    index = i + 1;
                    if (index >= pattern.length()) {
                        ch = (char) 0;
                    } else {
                        ch = pattern.charAt(index);
                    }
                    break;
                } else {
                    if (sb != null) {
                        sb.append(ch);
                    }
                }
            }
        }
        if (index != pattern.length() - 1) {
            throw new IllegalArgumentException("Expect terminating 's' or 'S'. source pattern is '" + pattern + "'");
        }
        if (ch == 's') {
            force = false;
        } else if (ch == 'S') {
            force = true;
        } else {
            throw new IllegalArgumentException("Expect terminating 's' or 'S'. source pattern is '" + pattern + "'");
        }
    }
}
