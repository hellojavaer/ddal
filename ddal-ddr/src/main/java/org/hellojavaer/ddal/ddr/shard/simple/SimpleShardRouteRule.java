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
package org.hellojavaer.ddal.ddr.shard.simple;

import org.hellojavaer.ddal.ddr.expression.el.function.ELFunctionManager;
import org.hellojavaer.ddal.ddr.expression.format.FormatExpression;
import org.hellojavaer.ddal.ddr.expression.format.FormatExpressionContext;
import org.hellojavaer.ddal.ddr.expression.format.simple.SimpleFormatExpressionContext;
import org.hellojavaer.ddal.ddr.expression.format.simple.SimpleFormatExpressionParser;
import org.hellojavaer.ddal.ddr.shard.ShardRouteContext;
import org.hellojavaer.ddal.ddr.shard.exception.ValueNotFoundException;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.ParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 15/11/2016.
 */
public class SimpleShardRouteRule implements Serializable {

    private String           scRoute;
    private String           scFormat;
    private String           tbRoute;
    private String           tbFormat;

    private Expression       scRouteExpression;
    private Expression       tbRouteExpression;

    private FormatExpression scFormatExpression;
    private FormatExpression tbFormatExpression;

    public String getScRoute() {
        return scRoute;
    }

    private String filter(String string) {
        if (string != null) {
            string = string.trim();
            if (string.length() == 0) {
                string = null;
            }
        }
        return string;
    }

    public void setScRoute(String scRoute) {
        scRoute = filter(scRoute);
        this.scRoute = scRoute;
        if (scRoute != null) {
            ExpressionParser parser = new SpelExpressionParser();
            this.scRouteExpression = parser.parseExpression(scRoute, PARSER_CONTEXT);
        }
    }

    public String getScFormat() {
        return scFormat;
    }

    public void setScFormat(String scFormat) {
        scFormat = filter(scFormat);
        this.scFormat = scFormat;
        if (scFormat != null) {
            SimpleFormatExpressionParser parser = new SimpleFormatExpressionParser();
            scFormatExpression = parser.parse(scFormat);
        }
    }

    public String getTbRoute() {
        return tbRoute;
    }

    public void setTbRoute(String tbRoute) {
        tbRoute = filter(tbRoute);
        this.tbRoute = tbRoute;
        if (tbRoute != null) {
            ExpressionParser parser = new SpelExpressionParser();

            this.tbRouteExpression = parser.parseExpression(tbRoute, PARSER_CONTEXT);
        }
    }

    public String getTbFormat() {
        return tbFormat;
    }

    public void setTbFormat(String tbFormat) {
        tbFormat = filter(tbFormat);
        this.tbFormat = tbFormat;
        if (tbFormat != null) {
            SimpleFormatExpressionParser parser = new SimpleFormatExpressionParser();
            tbFormatExpression = parser.parse(tbFormat);
        }
    }

    public Object parseScRoute(String scName, Object sdValue) {
        if (scRouteExpression == null) {
            return sdValue;
        } else {
            EvaluationContext context = buildEvaluationContext(scRoute);
            context.setVariable("scName", scName);
            context.setVariable("sdValue", sdValue);
            String $0 = scRouteExpression.getValue(context, String.class);
            return $0;
        }
    }

    public String parseScFormat(String scName, Object scRoute) {
        if (scFormatExpression == null) {
            return scName;
        } else {
            FormatExpressionContext context = new SimpleFormatExpressionContext();
            context.setVariable("scRoute", scRoute);
            context.setVariable("scName", scName);
            return scFormatExpression.getValue(context);
        }
    }

    public Object parseTbRoute(String tbName, Object sdValue) {
        if (tbRouteExpression == null) {
            return sdValue;
        } else {
            EvaluationContext context = buildEvaluationContext(tbRoute);
            context.setVariable("tbName", tbName);
            context.setVariable("sdValue", sdValue);
            String $0 = tbRouteExpression.getValue(context, String.class);
            return $0;
        }
    }

    public String parseTbFormat(String tbName, Object tbRoute) {
        if (tbFormatExpression == null) {
            return tbName;
        } else {
            FormatExpressionContext context = new SimpleFormatExpressionContext();
            context.setVariable("tbRoute", tbRoute);
            context.setVariable("tbName", tbName);
            return tbFormatExpression.getValue(context);
        }
    }

    private static final Set<String> RESERVED_WORDS = new HashSet<String>();
    static {
        RESERVED_WORDS.add("db");
        RESERVED_WORDS.add("dbName");
        RESERVED_WORDS.add("dbValue");
        RESERVED_WORDS.add("dbRoute");
        RESERVED_WORDS.add("dbFormat");

        RESERVED_WORDS.add("sc");
        RESERVED_WORDS.add("scName");
        RESERVED_WORDS.add("scValue");
        RESERVED_WORDS.add("scRoute");
        RESERVED_WORDS.add("scFormat");

        RESERVED_WORDS.add("tb");
        RESERVED_WORDS.add("tbName");
        RESERVED_WORDS.add("tbValue");
        RESERVED_WORDS.add("tbRoute");
        RESERVED_WORDS.add("tbFormat");

        RESERVED_WORDS.add("sd");
        RESERVED_WORDS.add("sdName");
        RESERVED_WORDS.add("sdKey");
        RESERVED_WORDS.add("sdValue");
        RESERVED_WORDS.add("sbRoute");
        RESERVED_WORDS.add("sbFormat");

        RESERVED_WORDS.add("col");
        RESERVED_WORDS.add("colName");
        RESERVED_WORDS.add("colValue");
        RESERVED_WORDS.add("colRoute");
        RESERVED_WORDS.add("colFormat");
    }

    private static boolean isReservedWords(String str) {
        if (str == null) {
            return false;
        } else {
            return RESERVED_WORDS.contains(str);
        }
    }

    private static EvaluationContext buildEvaluationContext(final String expression) {
        StandardEvaluationContext context = new StandardEvaluationContext() {

            @Override
            public Object lookupVariable(String name) {
                Object val = null;
                if (isReservedWords(name)) {
                    val = super.lookupVariable(name);
                    if (val == null) {
                        throw new ValueNotFoundException(
                                                         "Target value was not found for key '"
                                                                 + name
                                                                 + "' in system context when parsing route expression. Expression is '"
                                                                 + expression + "'");
                    }
                } else {
                    val = ELFunctionManager.getRegisteredFunction(name);
                    if (val == null) {
                        val = ShardRouteContext.getParameter(name);
                        if (val == null) {
                            throw new ValueNotFoundException(
                                                             "Target value was not found for key '"
                                                                     + name
                                                                     + "' in user parameter context when parsing routing expression. Expression is '"
                                                                     + expression + "'");
                        }
                    }
                }
                return val;
            }
        };
        return context;
    }

    private static ParserContext PARSER_CONTEXT = new ParserContext() {

                                                    public boolean isTemplate() {
                                                        return true;
                                                    }

                                                    public String getExpressionPrefix() {
                                                        return "{";
                                                    }

                                                    public String getExpressionSuffix() {
                                                        return "}";
                                                    }
                                                };
}
