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
package org.hellojavaer.ddr.core.text;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">zoukaiming[邹凯明]</a>,created on 16/11/2016.
 */
public class StringFormat {

    private String pattern;

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
            throw new IllegalStateException("string pattern must be start with %");
        }
        int s = 1;
        if (pattern.charAt(1) == '-') {
            append = true;
            s = 2;
        }
        int i = s;
        char ch = (char) 0;
        for (; i < pattern.length(); i++) {
            ch = pattern.charAt(i);
            if (ch == 's' || ch == 'S' || ch == '.') {
                break;
            } else {
                if (ch >= '0' && ch <= '9') {
                    this.length = (ch - '0') + length * 10;
                } else {
                    throw new IllegalStateException("StringFormat pattern expect digit after %");
                }
            }
        }
        if (s == i) {
            throw new IllegalStateException("StringFormat pattern expect digit after %");
        }
        if (ch == '.') {
            i++;
            s = i;
            StringBuilder sb = new StringBuilder();
            for (; i < pattern.length(); i++) {
                ch = pattern.charAt(i);
                if (ch == 's' || ch == 'S') {
                    break;
                } else if (ch >= '0' && ch <= '9') {// 0-9A-Za-z
                    sb.append(ch);
                    continue;
                } else if (ch == '\\') {
                    i++;
                    sb.append(pattern.charAt(i));
                    continue;
                } else {
                    throw new IllegalStateException("unexpected character '" + ch + "'");
                }
            }
            if (sb.length() == 0) {
                //TODO
                throw null;// 没前进
            }
            repeatString = sb.toString();
        }
        if (ch == 's') {
            force = false;
        } else if (ch == 'S') {
            force = true;
        } else {
            throw new IllegalStateException("unexpected end string '" + ch + "'");
        }
    }
}
