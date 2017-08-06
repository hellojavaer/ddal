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

    private Tokenizer  tokenizer = null;
    private List<List> list      = new ArrayList<>();
    private boolean    empty     = false;

    private class InnerRange {

        private boolean integerRange;
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

    public void visit(RangeExpressionItemVisitor itemVisitor) {
        if (list != null) {
            for (List segments : list) {
                visit0(segments, itemVisitor);
            }
        }
    }

    private void visit0(List segments, RangeExpressionItemVisitor itemVisitor) {
        if (segments == null || segments.isEmpty() || empty) {
            return;
        }
        if (segments.size() == 1) {
            Object obj = segments.get(0);
            if (obj instanceof List) {
                for (Object item : (List) obj) {
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
                }
            } else {
                itemVisitor.visit(obj);
            }
            return;
        } else {
            recvInvoke(segments, "", 0, itemVisitor);
        }
    }

    private void recvInvoke(List segments, String prefix, int index, RangeExpressionItemVisitor itemVisitor) {
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
                                    recvInvoke(segments, prefix + i, index + 1, itemVisitor);
                                }
                            } else {
                                for (int i = range.getBegin(); i >= range.getEnd(); i--) {
                                    recvInvoke(segments, prefix + i, index + 1, itemVisitor);
                                }
                            }
                        } else {
                            if (range.getBegin() <= range.getEnd()) {
                                for (int i = range.getBegin(); i <= range.getEnd(); i++) {
                                    recvInvoke(segments, prefix + ((char) i), index + 1, itemVisitor);
                                }
                            } else {
                                for (int i = range.getBegin(); i >= range.getEnd(); i--) {
                                    recvInvoke(segments, prefix + ((char) i), index + 1, itemVisitor);
                                }
                            }
                        }
                    } else {
                        recvInvoke(segments, prefix + item, index + 1, itemVisitor);
                    }
                }
            } else {
                recvInvoke(segments, prefix + obj, index + 1, itemVisitor);
            }
        }
    }

    // Expression -> (SplicedRange "," )(Expression | ∈)
    private void eatExpression() {
        List segments = new ArrayList();
        eatSplicedRange(segments);
        list.add(segments);
        if (tokenizer.hasMoreTokens()) {// ∈
            tokenizer.peekToken(0, TokenKind.COMMA);
            tokenizer.nextToken();
            eatExpression();
        }
    }

    // SplicedRange -> (<PLAIN_STR> | "[" Range "]")(SplicedRange | ∈)
    private void eatSplicedRange(List segments) {
        Token token = tokenizer.peekToken(0);
        if (token == null) {// ∈
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
                }
                tokenizer.peekToken(0, TokenKind.RSQUARE);
                break;
            default:
                return;
        }
        if (tokenizer.hasMoreTokens()) {// ∈
            tokenizer.nextToken();
            eatSplicedRange(segments);
        }
    }

    // Range -> ((LOOKAHEAD(2) <INT><TO><INT>|<INT>|<DOUBLE>|<STR>)) "," Range | ∈
    private void eatRange(List list) {
        Token token0 = tokenizer.peekToken(0);
        Token token1 = tokenizer.peekToken(1);
        if (token0 == null) {// ∈
            return;
        }
        if (token1 != null && token1.getKind() == TokenKind.TO) {
            if (token0.getKind() == TokenKind.LITERAL_INT) {
                Token token2 = tokenizer.nextToken(2, TokenKind.LITERAL_INT);
                InnerRange range = new InnerRange(true, Integer.parseInt(token0.getData()),
                                                  Integer.parseInt(token2.getData()));
                list.add(range);
            } else if (token0.getKind() == TokenKind.LITERAL_LOWER_CHAR) {
                Token token2 = tokenizer.nextToken(2, TokenKind.LITERAL_LOWER_CHAR);
                InnerRange range = new InnerRange(false, token0.getData().charAt(0), token2.getData().charAt(0));
                list.add(range);
            } else if (token0.getKind() == TokenKind.LITERAL_UPPER_CHAR) {
                Token token2 = tokenizer.nextToken(2, TokenKind.LITERAL_UPPER_CHAR);
                InnerRange range = new InnerRange(false, token0.getData().charAt(0), token2.getData().charAt(0));
                list.add(range);
            } else {
                tokenizer.peekToken(0, TokenKind.LITERAL_INT, TokenKind.LITERAL_LOWER_CHAR,
                                    TokenKind.LITERAL_UPPER_CHAR);
            }
        } else if (token0.getKind() == TokenKind.LITERAL_INT) {
            list.add(Integer.parseInt(token0.getData()));
        } else if (token0.getKind() == TokenKind.LITERAL_DOUBLE) {
            list.add(Double.parseDouble(token0.getData()));
        } else if (token0.getKind() == TokenKind.LITERAL_STRING || token0.getKind() == TokenKind.LITERAL_LOWER_CHAR
                   || token0.getKind() == TokenKind.LITERAL_UPPER_CHAR) {
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
