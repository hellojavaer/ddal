/*
 * Copyright 2017-2017 the original author or authors.
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

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 02/08/2017.
 */
class Tokenizer {

    private static final String BLANK_STRING   = "";
    private List<Token>         tokens         = new ArrayList<>();
    private int                 index          = 0;
    private int                 pos            = 0;

    private String              expression;

    private static final byte   FLAGS[]        = new byte[256];
    private static final byte   IS_DIGIT       = 0x01;
    private static final byte   IS_LOWER_ALPHA = 0x02;
    private static final byte   IS_UPPER_ALPHA = 0x04;

    static {
        for (int ch = '0'; ch <= '9'; ch++) {
            FLAGS[ch] |= IS_DIGIT;
        }
        for (int ch = 'a'; ch <= 'z'; ch++) {
            FLAGS[ch] |= IS_LOWER_ALPHA;
        }
        for (int ch = 'A'; ch <= 'Z'; ch++) {
            FLAGS[ch] |= IS_UPPER_ALPHA;
        }
    }

    public Tokenizer(String expression) {
        if (expression == null) {
            return;
        }
        this.expression = expression;
        boolean block = false;
        for (; pos < expression.length(); pos++) {
            char ch = expression.charAt(pos);
            if (block == false) {
                switch (ch) {
                    case '[':
                        block = true;
                        tokens.add(new Token(TokenKind.LSQUARE, pos, pos + 1));
                        break;
                    case ']':
                        throw new RangeExpressionException(expression, pos, "Unexpected character ']'");
                    case ',':
                        if (tokens.size() > 0) {
                            Token preToken = tokens.get(tokens.size() - 1);
                            if (preToken != null && preToken.getKind() == TokenKind.COMMA) {
                                tokens.add(new Token(TokenKind.LITERAL_STRING, BLANK_STRING, pos - 1, pos - 1));
                            }
                        } else {
                            tokens.add(new Token(TokenKind.LITERAL_STRING, BLANK_STRING, pos, pos));
                        }
                        tokens.add(new Token(TokenKind.COMMA, pos, pos + 1));
                        break;
                    default:
                        int begin = pos;
                        int end = begin;
                        for (; end < expression.length(); end++) {
                            char temp = expression.charAt(end);
                            if (temp == '[' || temp == ']' || temp == ',') {
                                break;
                            }
                        }
                        tokens.add(new Token(TokenKind.LITERAL_STRING, expression.substring(begin, end).trim(), begin,
                                             end));
                        pos = end - 1;
                        break;
                }
            } else {
                switch (ch) {
                    case '[':
                        throw new RangeExpressionException(expression, pos, "Unexpected character '['");
                    case ']':// 区块
                        tokens.add(new Token(TokenKind.RSQUARE, pos, pos + 1));
                        block = false;
                        break;
                    case ',': // 分组
                        tokens.add(new Token(TokenKind.COMMA, pos, pos + 1));
                        break;
                    case '.':// 区间
                        pos++;
                        if (pos < expression.length()) {
                            ch = expression.charAt(pos);
                            if (ch == '.') {
                                tokens.add(new Token(TokenKind.TO, pos, pos + 2));
                            } else {
                                throw new RangeExpressionException(expression, pos, "Expecte character '.'");
                            }
                        }
                        break;
                    case '\'':// 字符
                        pushString(false);
                        break;
                    case '"':
                        pushString(true);
                        break;
                    case '+':// 带符号数字
                    case '-':
                        pos++;
                        if (pos < expression.length()) {
                            ch = expression.charAt(pos);
                            if (isDisit(ch)) {
                                pushNum(true);
                            } else {
                                throw new RangeExpressionException(expression, pos, "Expecte digist");
                            }
                        }
                        break;
                    case '0':// 无符号数字
                    case '1':
                    case '2':
                    case '3':
                    case '4':
                    case '5':
                    case '6':
                    case '7':
                    case '8':
                    case '9':
                        pushNum(false);
                        break;
                    default:
                        throw new RangeExpressionException(expression, pos, "Unexpected character '" + ch + "'");
                }
            }
        }
        if (tokens.size() > 0) {
            if (block == false) {
                Token preToken = tokens.get(tokens.size() - 1);
                if (preToken != null && preToken.getKind() == TokenKind.COMMA) {
                    tokens.add(new Token(TokenKind.LITERAL_STRING, BLANK_STRING, pos - 1, pos - 1));
                }
            }
        } else {
            tokens.add(new Token(TokenKind.LITERAL_STRING, BLANK_STRING, 0, 0));
        }
    }

    private void pushString(boolean isDoubleuotes) {
        boolean escape = false;
        StringBuilder sb = null;
        for (int i = pos + 1; i < expression.length(); i++) {
            char ch = expression.charAt(i);
            if (escape) {
                escape = false;
                if (sb == null) {
                    sb = new StringBuilder();
                }
                sb.append(ch);
            } else {
                boolean terminal = false;
                if (ch == '\\') {
                    escape = true;
                    continue;
                } else if (isDoubleuotes && ch == '"') {
                    terminal = true;
                } else if (!isDoubleuotes && ch == '\'') {
                    terminal = true;
                } else {
                    if (sb != null) {
                        sb.append(ch);
                    }
                }
                if (terminal) {
                    String str;
                    if (sb == null) {
                        str = expression.substring(pos + 1, i);
                    } else {
                        str = sb.toString();
                    }
                    if (str.length() == 1 && (FLAGS[str.charAt(0)] & IS_LOWER_ALPHA) != 0) {
                        tokens.add(new Token(TokenKind.LITERAL_LOWER_CHAR, str, pos + 1, i));
                    } else if (str.length() == 1 && (FLAGS[str.charAt(0)] & IS_UPPER_ALPHA) != 0) {
                        tokens.add(new Token(TokenKind.LITERAL_UPPER_CHAR, str, pos + 1, i));
                    } else {
                        tokens.add(new Token(TokenKind.LITERAL_STRING, str, pos + 1, i));
                    }
                    pos = i;
                    return;
                }
            }
        }
        if (isDoubleuotes) {
            throw new RangeExpressionException(expression, pos, "Cannot find terminating \" for string");
        } else {
            throw new RangeExpressionException(expression, pos, "Cannot find terminating ' for string");
        }
    }

    private void pushNum(boolean signed) {
        for (int i = pos; i < expression.length(); i++) {
            char ch = expression.charAt(i);
            if ((FLAGS[ch] & IS_DIGIT) == 0) {
                if (ch == '.' && i + 1 < i + expression.length() && expression.charAt(i + 1) != '.') {
                    for (int j = i + 1; j < expression.length(); j++) {
                        ch = expression.charAt(j);
                        if (!isDisit(ch)) {
                            if (signed == false) {
                                tokens.add(new Token(TokenKind.LITERAL_DOUBLE, expression.substring(pos, j), pos, j));
                            } else {
                                tokens.add(new Token(TokenKind.LITERAL_DOUBLE, expression.substring(pos - 1, j),
                                                     pos - 1, j));
                            }
                            pos = j - 1;
                            return;
                        }
                    }
                } else {
                    if (signed == false) {
                        tokens.add(new Token(TokenKind.LITERAL_INT, expression.substring(pos, i), pos, i));
                    } else {
                        tokens.add(new Token(TokenKind.LITERAL_INT, expression.substring(pos - 1, i), pos - 1, i));
                    }
                    pos = i - 1;
                    return;
                }
            }
        }
    }

    private boolean isDisit(char ch) {
        return (FLAGS[ch] & IS_DIGIT) == 1;
    }

    public Token peekToken(int nextPos, TokenKind... expectedTokenKinds) {
        int pos = nextPos + index;
        Token token = null;
        if (pos >= 0 && pos < tokens.size()) {
            token = tokens.get(pos);
        }
        assertEquals(token, expectedTokenKinds);
        return token;
    }

    public Token nextToken(TokenKind... expectedTokenKinds) {
        return nextToken(1, expectedTokenKinds);
    }

    public Token nextToken(int nextPos, TokenKind... expectedTokenKinds) {
        index = nextPos + index;
        Token token = null;
        if (index < tokens.size()) {
            token = tokens.get(index);
        }
        assertEquals(token, expectedTokenKinds);
        return token;
    }

    private void assertEquals(Token token, TokenKind... expectedTokenKinds) {
        if (expectedTokenKinds == null || expectedTokenKinds.length == 0) {
            return;
        }
        if (token != null) {
            for (TokenKind kind : expectedTokenKinds) {
                if (token.getKind() == kind) {
                    return;
                }
            }
            throw new RangeExpressionException(expression, token.getStartPos(), "Current token kind is "
                                                                                + token.getKind() + ", expecte "
                                                                                + buildString(expectedTokenKinds));
        } else {
            throw new RangeExpressionException(expression, expression.length(), "Current token is null, expecte "
                                                                                + buildString(expectedTokenKinds));
        }
    }

    private String buildString(Object... objects) {
        if (objects == null) {
            return null;
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append('[');
            for (Object obj : objects) {
                sb.append(obj);
                sb.append(',');
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append(']');
            return sb.toString();
        }

    }

    public boolean hasMoreTokens() {
        return index < tokens.size() - 1;
    }

}
