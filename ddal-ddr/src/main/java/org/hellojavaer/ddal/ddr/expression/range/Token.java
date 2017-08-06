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

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 02/08/2017.
 */
class Token {

    private TokenKind kind;

    private String    data;

    private int       startPos;

    private int       endPos;

    public Token(TokenKind tokenKind, int startPos, int endPos) {
        this.kind = tokenKind;
        this.startPos = startPos;
        this.endPos = endPos;
    }

    public Token(TokenKind tokenKind, String data, int startPos, int endPos) {
        this(tokenKind, startPos, endPos);
        this.data = data;
    }

    public TokenKind getKind() {
        return this.kind;
    }

    public String getData() {
        return data;
    }

    @Override
    public String toString() {
        StringBuilder s = new StringBuilder();
        s.append("[").append(this.kind.toString());
        if (this.data != null) {
            s.append(":").append(this.data);
        }
        s.append("]");
        s.append("(").append(this.startPos).append(",").append(this.endPos).append(")");
        return s.toString();
    }

    public int getStartPos() {
        return startPos;
    }

    public int getEndPos() {
        return endPos;
    }

}
