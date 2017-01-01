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
package org.hellojavaer.ddr.expression.format.ast.sytax;

import org.hellojavaer.ddr.expression.format.StringFormat;
import org.hellojavaer.ddr.expression.format.ast.token.FeToken;
import org.hellojavaer.ddr.expression.format.ast.token.FeTokenParser;
import org.hellojavaer.ddr.expression.format.ast.token.FeTokenType;
import org.hellojavaer.ddr.expression.format.exception.FormatExpressionException;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 18/11/2016.
 */
public class FeSyntaxParser {

    public static FeCompoundExpression parse(String str) {
        FeCompoundExpression compoundExpression = new FeCompoundExpression();
        FeTokenParser tokenParser = new FeTokenParser(str);
        List<FeNodeImpl> nodes = new ArrayList<FeNodeImpl>();
        for (FeToken token = tokenParser.next(); token.getType() != FeTokenType.NULL; token = tokenParser.next()) {
            if (token.getType() == FeTokenType.PLAIN_TEXT) {
                nodes.add(new FePlainText((String) token.getData()));
            } else if (token.getType() == FeTokenType.LCURLY) {
                token = tokenParser.next();
                FeToken fromToken = token;
                assert0(str, token, FeTokenType.NUMBER, FeTokenType.STRING, FeTokenType.VAR);
                List<StringFormat> formats = new ArrayList<StringFormat>();
                token = tokenParser.next();
                while (token != null && token.getType() != FeTokenType.RCURLY) {
                    assert0(str, token, FeTokenType.COLON);
                    token = tokenParser.next();
                    assert0(str, token, FeTokenType.STRING);
                    formats.add(new StringFormat((String) token.getData()));
                    token = tokenParser.next();
                }
                assert0(str, token, FeTokenType.RCURLY);
                nodes.add(new FeFormater(fromToken, formats));
            } else {
                throw new FormatExpressionException("Unexpected token " + token.toString() + " at index "
                                                    + token.getStartPos() + ". Source string is " + str);
            }
        }
        compoundExpression.setChildren(nodes);
        return compoundExpression;
    }

    private static void assert0(String str, FeToken curToken, FeTokenType... expTypes) {
        if (curToken != null) {
            for (FeTokenType type : expTypes) {
                if (curToken.getType() == type) {
                    return;
                }
            }
        }
        StringBuilder sb = new StringBuilder();
        sb.append("Unexpected token type ");
        sb.append(curToken.getType());
        sb.append(" at index ");
        sb.append(curToken.getStartPos());
        sb.append(", expect ");
        for (FeTokenType type : expTypes) {
            sb.append(type.toString());
            sb.append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(". Source string is ");
        sb.append(str);
        throw new FormatExpressionException(sb.toString());
    }
}
