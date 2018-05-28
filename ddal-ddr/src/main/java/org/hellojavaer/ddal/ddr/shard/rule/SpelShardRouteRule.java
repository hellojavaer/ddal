/*
 * Copyright 2017-2018 the original author or authors.
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
package org.hellojavaer.ddal.ddr.shard.rule;

import org.hellojavaer.ddal.ddr.expression.ShardRouteRuleExpressionContext;
import org.hellojavaer.ddal.ddr.expression.el.spel.DDRSpelEvaluationContext;
import org.hellojavaer.ddal.ddr.shard.RangeShardValue;
import org.hellojavaer.ddal.ddr.shard.ShardRouteInfo;
import org.hellojavaer.ddal.ddr.shard.ShardRouteRule;
import org.hellojavaer.ddal.ddr.shard.exception.CrossTableException;
import org.hellojavaer.ddal.ddr.shard.exception.ExpressionValueNotFoundException;
import org.hellojavaer.ddal.ddr.shard.exception.OutOfRangeSizeLimitException;
import org.hellojavaer.ddal.ddr.utils.DDRToStringBuilder;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import java.util.*;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 25/04/2017.
 */
public class SpelShardRouteRule implements ShardRouteRule {

    private String     scRouteRule;
    private String     tbRouteRule;
    private Integer    rangeSizeLimit;

    private Expression scRouteRuleExpression;
    private Expression tbRouteRuleExpression;

    // used for spring bean
    private SpelShardRouteRule() {
    }

    public SpelShardRouteRule(String scRouteRule, String tbRouteRule) {
        setScRouteRule(scRouteRule);
        setTbRouteRule(tbRouteRule);
    }

    public SpelShardRouteRule(String scRouteRule, String tbRouteRule, Integer rangeSizeLimit) {
        this.scRouteRule = scRouteRule;
        this.tbRouteRule = tbRouteRule;
        this.rangeSizeLimit = rangeSizeLimit;
    }

    public void setScRouteRule(String scRouteRule) {
        scRouteRule = filter(scRouteRule);
        this.scRouteRule = scRouteRule;
        if (scRouteRule != null) {
            ExpressionParser parser = new SpelExpressionParser();
            this.scRouteRuleExpression = parser.parseExpression(scRouteRule, DDRSpelEvaluationContext.PARSER_CONTEXT);
        }
    }

    public String getScRouteRule() {
        return scRouteRule;
    }

    public void setTbRouteRule(String tbRouteRule) {
        tbRouteRule = filter(tbRouteRule);
        this.tbRouteRule = tbRouteRule;
        if (tbRouteRule != null) {
            ExpressionParser parser = new SpelExpressionParser();
            this.tbRouteRuleExpression = parser.parseExpression(tbRouteRule, DDRSpelEvaluationContext.PARSER_CONTEXT);
        }
    }

    public String getTbRouteRule() {
        return tbRouteRule;
    }

    public Integer getRangeSizeLimit() {
        return rangeSizeLimit;
    }

    public void setRangeSizeLimit(Integer rangeSizeLimit) {
        this.rangeSizeLimit = rangeSizeLimit;
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

    @Override
    public String parseScName(String scName, Object sdValue) {
        if (scRouteRuleExpression == null) {
            return scName;
        } else {
            EvaluationContext elContext = buildEvaluationContext(tbRouteRule);
            elContext.setVariable("scName", scName);
            return parseName(scRouteRuleExpression, elContext, sdValue);
        }
    }

    @Override
    public String parseTbName(String tbName, Object sdValue) {
        if (tbRouteRuleExpression == null) {
            return tbName;
        } else {
            EvaluationContext elContext = buildEvaluationContext(tbRouteRule);
            elContext.setVariable("tbName", tbName);
            return parseName(tbRouteRuleExpression, elContext, sdValue);
        }
    }

    @Override
    public Map<ShardRouteInfo, List<RangeShardValue>> groupSdValuesByRouteInfo(String scName, String tbName,
                                                                               RangeShardValue rangeShardValue) {
        Long begin = rangeShardValue.getBegin();
        Long end = rangeShardValue.getEnd();
        if (begin == null || end == null) {
            throw new IllegalArgumentException("rangeShardValue.begin and rangeShardValue.end can't be null");
        }
        if (begin > end) {
            throw new IllegalArgumentException("rangeShardValue.begin can't be greater than rangeShardValue.end");
        }
        if (rangeSizeLimit != null && end - begin + 1 > rangeSizeLimit) {
            throw new OutOfRangeSizeLimitException((end - begin) + " > " + rangeSizeLimit);
        }
        Map<ShardRouteInfo, List<RangeShardValue>> map = new LinkedHashMap<>();
        if (scRouteRuleExpression == null && tbRouteRuleExpression == null) {
            List<RangeShardValue> list = new ArrayList(1);
            list.add(new RangeShardValue(begin, end));
            ShardRouteInfo routeInfo = new ShardRouteInfo(scName, tbName);
            map.put(routeInfo, list);
            return map;
        }
        for (long l = begin; l <= end; l++) {
            String scName0 = parseScName(scName, l);
            String tbName0 = parseTbName(tbName, l);
            ShardRouteInfo routeInfo = new ShardRouteInfo();
            routeInfo.setScName(scName0);
            routeInfo.setTbName(tbName0);
            List<RangeShardValue> rangeShardValues = map.get(routeInfo);
            if (rangeShardValues == null) {
                rangeShardValues = new ArrayList<>();
                map.put(routeInfo, rangeShardValues);
            }
            rangeShardValues.add(new RangeShardValue(l, l));
        }
        return map;
    }

    protected String parseName(Expression expression, EvaluationContext elContext, Object sdValue) {
        if (expression == null) {
            throw new IllegalArgumentException("expression can't be null");
        }
        if (sdValue != null && sdValue instanceof RangeShardValue) {
            Long begin = ((RangeShardValue) sdValue).getBegin();
            Long end = ((RangeShardValue) sdValue).getEnd();
            if (begin == null || end == null) {
                throw new IllegalArgumentException("rangeShardValue.begin and rangeShardValue.end can't be null");
            }
            if (begin > end) {
                throw new IllegalArgumentException("rangeShardValue.begin can't be greater than rangeShardValue.end");
            }
            if (rangeSizeLimit != null && end - begin + 1 > rangeSizeLimit) {
                throw new OutOfRangeSizeLimitException((end - begin) + " > " + rangeSizeLimit);
            }
            String result = null;
            for (long l = begin; l <= end; l++) {
                elContext.setVariable("sdValue", l);
                String temp = expression.getValue(elContext, String.class);
                if (result != null && !result.equals(temp)) {
                    throw new CrossTableException(result + " and " + temp);
                }
                result = temp;
            }
            return result;
        } else {
            elContext.setVariable("sdValue", sdValue);
            return expression.getValue(elContext, String.class);
        }
    }

    @Override
    public String toString() {
        return new DDRToStringBuilder()//
        .append("scRouteRule", scRouteRule)//
        .append("tbRouteRule", tbRouteRule)//
        .toString();
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
        RESERVED_WORDS.add("sdRoute");
        RESERVED_WORDS.add("sdFormat");

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

    /**
     * load order
     * 1.reserved words
     * 2.function
     * 3.user-define var
     */
    private static EvaluationContext buildEvaluationContext(final String expression) {
        return new DDRSpelEvaluationContext() {

            @Override
            public Object lookupVariable(String name) {
                Object val = null;
                if (isReservedWords(name)) {
                    val = super.lookupVariable(name);
                    if (val == null) {
                        throw new ExpressionValueNotFoundException("Value of '" + name
                                                                   + "' is not found when parsing expression '"
                                                                   + expression + "'");
                    }
                } else {
                    val = ShardRouteRuleExpressionContext.lookupVariable(name);
                }
                return val;
            }
        };
    }

}
