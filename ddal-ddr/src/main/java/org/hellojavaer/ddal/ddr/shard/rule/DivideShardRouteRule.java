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
package org.hellojavaer.ddal.ddr.shard.rule;

import org.hellojavaer.ddal.ddr.shard.RangeShardValue;
import org.hellojavaer.ddal.ddr.shard.ShardRouteInfo;
import org.hellojavaer.ddal.ddr.shard.ShardRouteRule;
import org.hellojavaer.ddal.ddr.shard.ShardRouteRuleContext;
import org.hellojavaer.ddal.ddr.shard.exception.CrossTableException;
import org.hellojavaer.ddal.ddr.shard.exception.UnsupportedShardValueTypeException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 16/09/2017.
 */
public class DivideShardRouteRule implements ShardRouteRule {

    private Long scSdValueDividend;
    private Long tbSdValueDividend;

    private DivideShardRouteRule() {
    }

    public DivideShardRouteRule(Long scSdValueDividend, Long tbSdValueDividend) {
        this.scSdValueDividend = scSdValueDividend;
        this.tbSdValueDividend = tbSdValueDividend;
    }

    public Long getScSdValueDividend() {
        return scSdValueDividend;
    }

    private void setScSdValueDividend(Long scSdValueDividend) {
        this.scSdValueDividend = scSdValueDividend;
    }

    public Long getTbSdValueDividend() {
        return tbSdValueDividend;
    }

    private void setTbSdValueDividend(Long tbSdValueDividend) {
        this.tbSdValueDividend = tbSdValueDividend;
    }

    @Override
    public String parseScName(ShardRouteRuleContext context) {
        return parseName(context.getScName(), context.getSdValue(), scSdValueDividend);
    }

    @Override
    public String parseTbName(ShardRouteRuleContext context) {
        return parseName(context.getTbName(), context.getSdValue(), tbSdValueDividend);
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
        int scBegin = (int) (begin / scSdValueDividend);
        int scEnd = (int) (end / scSdValueDividend);
        int tbBegin = (int) (begin / tbSdValueDividend);
        int tbEnd = (int) (end / tbSdValueDividend);
        Map<ShardRouteInfo, List<RangeShardValue>> map = new HashMap<>();
        if (scName != null) {
            for (int i = scBegin; i <= scEnd; i++) {
                String scName0 = scName + '_' + i;
                for (int j = tbBegin; j < tbEnd; j++) {
                    String tbName0 = tbName + '_' + j;
                    ShardRouteInfo routeInfo = new ShardRouteInfo(scName0, tbName0);
                    List<RangeShardValue> list = map.get(routeInfo);
                    if (list == null) {
                        list = new ArrayList<>();
                        map.put(routeInfo, list);
                    }
                    RangeShardValue rangeShardValue1 = new RangeShardValue(j * tbSdValueDividend, (j + 1)
                                                                                                  * tbSdValueDividend
                                                                                                  - 1);
                    if (rangeShardValue1.getBegin() <= rangeShardValue.getBegin()
                        && rangeShardValue1.getEnd() <= rangeShardValue.getBegin()) {
                        rangeShardValue1.setBegin(rangeShardValue.getBegin());
                    }
                    if (rangeShardValue1.getBegin() <= rangeShardValue.getEnd()
                        && rangeShardValue1.getEnd() <= rangeShardValue.getEnd()) {
                        rangeShardValue1.setEnd(rangeShardValue.getEnd());
                    }
                    list.add(rangeShardValue1);
                }
            }
        } else {
            for (int j = tbBegin; j < tbEnd; j++) {
                String tbName0 = tbName + '_' + j;
                ShardRouteInfo routeInfo = new ShardRouteInfo(null, tbName0);
                List<RangeShardValue> list = map.get(routeInfo);
                if (list == null) {
                    list = new ArrayList<>();
                    map.put(routeInfo, list);
                }
                RangeShardValue rangeShardValue1 = new RangeShardValue(j * tbSdValueDividend, (j + 1)
                                                                                              * tbSdValueDividend - 1);
                if (rangeShardValue1.getBegin() <= rangeShardValue.getBegin()
                    && rangeShardValue1.getEnd() <= rangeShardValue.getBegin()) {
                    rangeShardValue1.setBegin(rangeShardValue.getBegin());
                }
                if (rangeShardValue1.getBegin() <= rangeShardValue.getEnd()
                    && rangeShardValue1.getEnd() <= rangeShardValue.getEnd()) {
                    rangeShardValue1.setEnd(rangeShardValue.getEnd());
                }
                list.add(rangeShardValue1);
            }
        }
        return map;
    }

    protected String parseName(String name, Object sdValue, Long dividend) {
        if (dividend == null) {
            return name;
        }
        if (sdValue == null) {
            return null;
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append(name).append('_');
            if (sdValue instanceof Number) {
                long l = ((Number) sdValue).longValue();
                return sb.append(l / dividend).toString();
            } else if (sdValue instanceof RangeShardValue) {
                Long begin = ((RangeShardValue) sdValue).getBegin();
                Long end = ((RangeShardValue) sdValue).getEnd();
                if (begin == null || end == null) {
                    throw new IllegalArgumentException("rangeShardValue.begin and rangeShardValue.end can't be null");
                }
                if (begin > end) {
                    throw new IllegalArgumentException(
                                                       "rangeShardValue.begin can't be greater than rangeShardValue.end");
                }
                Long a = begin / dividend;
                Long b = end / dividend;
                if (a != b) {
                    String prefix = sb.toString();
                    throw new CrossTableException(prefix + a + " and " + prefix + b);
                } else {
                    return sb.append(a).toString();
                }
            } else if (sdValue instanceof String) {
                Long l = Long.valueOf((String) sdValue);
                return sb.append(l / dividend).toString();
            } else {
                throw new UnsupportedShardValueTypeException(sdValue.getClass().toString());
            }
        }
    }
}
