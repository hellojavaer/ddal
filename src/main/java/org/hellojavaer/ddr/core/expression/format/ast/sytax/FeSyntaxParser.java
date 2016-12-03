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
package org.hellojavaer.ddr.core.expression.format.ast.sytax;

import org.hellojavaer.ddr.core.expression.format.ast.token.FeToken;
import org.hellojavaer.ddr.core.expression.format.ast.token.FeTokenParser;
import org.hellojavaer.ddr.core.expression.format.ast.token.FeTokenType;
import org.hellojavaer.ddr.core.text.StringFormat;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">zoukaiming[邹凯明]</a>,created on 18/11/2016.
 */
public class FeSyntaxParser {

    public static CompoundExpression parse(String str) {
        CompoundExpression compoundExpression = new CompoundExpression();
        FeTokenParser tokenParser = new FeTokenParser(str);
        List<FeNodeImpl> nodes = new ArrayList<FeNodeImpl>();
        for (FeToken token = tokenParser.next(); token != null; token = tokenParser.next()) {
            if (token.getType() == FeTokenType.PLAIN_TEXT) {
                nodes.add(new PlainText((String) token.getData()));
            } else if (token.getType() == FeTokenType.LCURLY) {
                token = tokenParser.next();
                FeToken fromToken = token;
                assert0(token, FeTokenType.NUMBER, FeTokenType.STRING, FeTokenType.VAR);
                List<StringFormat> formats = new ArrayList<StringFormat>();
                token = tokenParser.next();
                while (token != null && token.getType() != FeTokenType.RCURLY) {
                    assert0(token, FeTokenType.COLON);
                    token = tokenParser.next();
                    assert0(token, FeTokenType.FORMAT_PATTERN);
                    formats.add((StringFormat) token.getData());
                    token = tokenParser.next();
                }
                assert0(token, FeTokenType.RCURLY);
                nodes.add(new FeFormater(fromToken, formats));
            } else {
                throw new IllegalStateException("");
            }
        }
        compoundExpression.setChildren(nodes);
        return compoundExpression;
    }

    private static void assert0(FeToken curToken, FeTokenType... expTypes) {
        if (curToken == null) {
            throw new IllegalStateException("");
        }
        for (FeTokenType type : expTypes) {
            if (curToken.getType() == type) {
                return;
            }
        }
        throw new IllegalStateException("");
    }
}
