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
 * range expression
 * 
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 02/08/2017.
 */
public class RangeExpressionParser {

    private Tokenizer tokenizer = null;
    private List      segments  = new ArrayList<>();
    private boolean   empty     = false;

    private class InnerRange {

        private boolean integerRange; // 0:int,1:char
        private int     begin;
        private int     end;

        public InnerRange(boolean integerRange, int begin, int end) {
            this.integerRange = integerRange;
            this.begin = begin;
            this.end = end;
        }

        public boolean isIntegerRange() {
            return integerRange;
        }

        public void setIntegerRange(boolean integerRange) {
            this.integerRange = integerRange;
        }

        public int getBegin() {
            return begin;
        }

        public void setBegin(int begin) {
            this.begin = begin;
        }

        public int getEnd() {
            return end;
        }

        public void setEnd(int end) {
            this.end = end;
        }
    }

    public RangeExpressionParser(String expression) {
        this.tokenizer = new Tokenizer(expression);
        eatExpression();
    }

    public void visit(RangeItemVisitor itemVisitor) {
        if (segments == null || segments.isEmpty() || empty) {
            return;
        }
        if (segments.size() == 1) {
            Object item = segments.get(0);
            if (item instanceof InnerRange) {
                InnerRange range = (InnerRange) item;
                if (range.isIntegerRange()) {
                    if (range.getBegin() <= range.getEnd()) {
                        for (int i = range.getBegin(); i <= range.getEnd(); i++) {
                            itemVisitor.visit(i);
                        }
                    } else {
                        for (int i = range.getBegin(); i >= range.getEnd(); i--) {
                            itemVisitor.visit(i);
                        }
                    }
                } else {
                    if (range.getBegin() <= range.getEnd()) {
                        for (int i = range.getBegin(); i <= range.getEnd(); i++) {
                            itemVisitor.visit(String.valueOf((char) i));
                        }
                    } else {
                        for (int i = range.getBegin(); i >= range.getEnd(); i--) {
                            itemVisitor.visit(String.valueOf((char) i));
                        }
                    }
                }
            } else {
                itemVisitor.visit(item);
            }
            return;
        } else {
            recvInvoke("", 0, itemVisitor);
        }
    }

    private void recvInvoke(String prefix, int index, RangeItemVisitor itemVisitor) {
        if (index == segments.size()) {
            itemVisitor.visit(prefix);
        } else {
            Object obj = segments.get(index);
            if (obj instanceof List) {
                for (Object item : (List) obj) {
                    if (item instanceof InnerRange) {
                        InnerRange range = (InnerRange) item;
                        if (range.isIntegerRange()) {
                            if (range.getBegin() <= range.getEnd()) {
                                for (int i = range.getBegin(); i <= range.getEnd(); i++) {
                                    recvInvoke(prefix + i, index + 1, itemVisitor);
                                }
                            } else {
                                for (int i = range.getBegin(); i >= range.getEnd(); i--) {
                                    recvInvoke(prefix + i, index + 1, itemVisitor);
                                }
                            }
                        } else {
                            if (range.getBegin() <= range.getEnd()) {
                                for (int i = range.getBegin(); i <= range.getEnd(); i++) {
                                    recvInvoke(prefix + ((char) i), index + 1, itemVisitor);
                                }
                            } else {
                                for (int i = range.getBegin(); i >= range.getEnd(); i--) {
                                    recvInvoke(prefix + ((char) i), index + 1, itemVisitor);
                                }
                            }
                        }
                    } else {
                        recvInvoke(prefix + item, index + 1, itemVisitor);
                    }
                }
            } else {
                recvInvoke(prefix + obj, index + 1, itemVisitor);
            }
        }
    }

    // S -> (<PLAIN_STR> | "[" Range "]")(S | e)
    private void eatExpression() {
        Token token = tokenizer.peekToken(0);
        if (token == null) {
            return;
        }
        switch (token.getKind()) {
            case LITERAL_STRING:
                segments.add(token.getData());
                break;
            case LSQUARE:
                tokenizer.nextToken();
                List<?> list = new ArrayList<>();
                eatRange(list);
                if (list.isEmpty()) {
                    empty = true;
                } else {
                    segments.add(list);
                    tokenizer.peekToken(0, TokenKind.RSQUARE);
                }
                break;
            default:
                throw null;
        }
        if (tokenizer.hasMoreTokens()) {
            tokenizer.nextToken();
            eatExpression();
        }
    }

    // Range -> ((LOOKAHEAD(2) <INT><TO><INT>|<INT>|<DOUBLE>|<STR>)) "," Range | e
    private void eatRange(List list) {
        Token token0 = tokenizer.peekToken(0);
        Token token1 = tokenizer.peekToken(1);
        if (token1 != null && token1.getKind() == TokenKind.TO) {
            if (token0 != null && token0.getKind() == TokenKind.LITERAL_INT) {
                Token token2 = tokenizer.nextToken(2, TokenKind.LITERAL_INT);
                InnerRange range = new InnerRange(true, Integer.parseInt(token0.getData()),
                                                  Integer.parseInt(token2.getData()));
                list.add(range);
            } else if (token0 != null && token0.getKind() == TokenKind.LITERAL_LOWER_CHAR) {
                Token token2 = tokenizer.nextToken(2, TokenKind.LITERAL_LOWER_CHAR);
                InnerRange range = new InnerRange(false, token0.getData().charAt(0), token2.getData().charAt(0));
                list.add(range);
            } else if (token0 != null && token0.getKind() == TokenKind.LITERAL_UPPER_CHAR) {
                Token token2 = tokenizer.nextToken(2, TokenKind.LITERAL_UPPER_CHAR);
                InnerRange range = new InnerRange(false, token0.getData().charAt(0), token2.getData().charAt(0));
                list.add(range);
            } else {
                throw null;
            }
        } else if (token0.getKind() == TokenKind.LITERAL_INT) {
            list.add(Integer.parseInt(token0.getData()));
        } else if (token0.getKind() == TokenKind.LITERAL_DOUBLE) {
            list.add(Double.parseDouble(token0.getData()));
        } else if (token0.getKind() == TokenKind.LITERAL_STRING) {
            list.add(token0.getData());
        } else {
            return;
        }
        Token token = tokenizer.nextToken();
        if (token != null && token.getKind() == TokenKind.COMMA) {
            tokenizer.nextToken();
            eatRange(list);
        }
    }

}
