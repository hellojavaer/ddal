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
package org.hellojavaer.ddr.core.expression.format.simple;

import org.hellojavaer.ddr.core.expression.format.FormatExpression;
import org.hellojavaer.ddr.core.expression.format.FormatExpressionContext;
import org.hellojavaer.ddr.core.expression.format.ast.sytax.FeCompoundExpression;
import org.hellojavaer.ddr.core.expression.format.FormatExpressionParser;
import org.hellojavaer.ddr.core.expression.format.ast.sytax.FeSyntaxParser;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 15/11/2016.
 */
public class SimpleFormatExpressionParser implements FormatExpressionParser {

    public FormatExpression parse(String str) {
        final FeCompoundExpression compoundExpression = FeSyntaxParser.parse(str);
        FormatExpression formatExpression = new FormatExpression() {
            @Override
            public String getValue(FormatExpressionContext context) {
                return compoundExpression.getValue(context);
            }
        };
        return formatExpression;
    }
}
