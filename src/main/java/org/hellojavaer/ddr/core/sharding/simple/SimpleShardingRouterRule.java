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
package org.hellojavaer.ddr.core.sharding.simple;

import org.hellojavaer.ddr.core.expression.el.functionkit.ELFunctionKitManager;
import org.hellojavaer.ddr.core.expression.format.FormatExpression;
import org.hellojavaer.ddr.core.expression.format.FormatExpressionContext;
import org.hellojavaer.ddr.core.expression.format.simple.SimpleFormatExpressionContext;
import org.hellojavaer.ddr.core.expression.format.simple.SimpleFormatExpressionParser;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.ParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Map;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">zoukaiming[邹凯明]</a>,created on 15/11/2016.
 */
public class SimpleShardingRouterRule implements Serializable {

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
        if (scName == null) {
            return null;
        } else {
            if (scRouteExpression == null) {
                return "";
            } else {
                EvaluationContext context = buildEvaluationContext();
                context.setVariable("scName", scName);
                context.setVariable("sdValue", sdValue);
                String $0 = scRouteExpression.getValue(context, String.class);
                return $0;
            }
        }
    }

    public String parseScFormat(String scName, Object $0) {
        if ($0 == null) {
            return null;
        } else {
            if (scFormatExpression == null) {
                return $0.toString();
            } else {
                FormatExpressionContext context = new SimpleFormatExpressionContext();
                context.setVariable("scRoute", $0);
                context.setVariable("scName", scName);
                return scFormatExpression.getValue(context);
            }
        }
    }

    public Object parseTbRoute(String tbName, Object sdValue) {
        if (tbName == null) {
            return null;
        } else {
            if (tbRouteExpression == null) {
                return tbName;
            } else {
                EvaluationContext context = buildEvaluationContext();
                context.setVariable("tbName", tbName);
                context.setVariable("sdValue", sdValue);
                String $0 = tbRouteExpression.getValue(context, String.class);
                return $0;
            }
        }
    }

    public String parseTbFormat(String tbName, Object $0) {
        if ($0 == null) {
            return null;
        } else {
            if (tbFormatExpression == null) {
                return $0.toString();
            } else {
                FormatExpressionContext context = new SimpleFormatExpressionContext();
                context.setVariable("tbRoute", $0);
                context.setVariable("tbName", tbName);
                return tbFormatExpression.getValue(context);
            }
        }
    }

    private static EvaluationContext buildEvaluationContext() {
        StandardEvaluationContext context = new StandardEvaluationContext();
        for (Map.Entry<String, Method> entry : ELFunctionKitManager.getRegisteredFunctions()) {
            context.registerFunction(entry.getKey(), entry.getValue());
        }
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
