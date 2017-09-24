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
import org.hellojavaer.ddal.ddr.shard.exception.CrossTableException;
import org.hellojavaer.ddal.ddr.shard.exception.UnsupportedShardValueTypeException;

import java.util.*;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 16/09/2017.
 */
public class DivideShardRouteRule implements ShardRouteRule {

    private Long    scSdValueDividend;
    private Long    tbSdValueDividend;
    private boolean useOrignalNameIfZero = false;

    private DivideShardRouteRule() {
    }

    public DivideShardRouteRule(Long scSdValueDividend, Long tbSdValueDividend) {
        this.scSdValueDividend = scSdValueDividend;
        this.tbSdValueDividend = tbSdValueDividend;
        verify();
    }

    public DivideShardRouteRule(Long scSdValueDividend, Long tbSdValueDividend, boolean useOrignalNameIfZero) {
        this.scSdValueDividend = scSdValueDividend;
        this.tbSdValueDividend = tbSdValueDividend;
        this.useOrignalNameIfZero = useOrignalNameIfZero;
        verify();
    }

    private void verify() {
        if (scSdValueDividend != null && scSdValueDividend <= 0) {
            throw new IllegalArgumentException("scSdValueDividend must be greater than 0");
        }
        if (tbSdValueDividend != null && tbSdValueDividend <= 0) {
            throw new IllegalArgumentException("tbSdValueDividend must be greater than 0");
        }
        if (scSdValueDividend != null && tbSdValueDividend != null) {
            if (tbSdValueDividend > scSdValueDividend || scSdValueDividend % tbSdValueDividend != 0) {
                throw new IllegalArgumentException("tbSdValueDividend must be a multiple of scSdValueDividend");
            }
        }
    }

    public Long getScSdValueDividend() {
        return scSdValueDividend;
    }

    private void setScSdValueDividend(Long scSdValueDividend) {
        this.scSdValueDividend = scSdValueDividend;
        verify();
    }

    public Long getTbSdValueDividend() {
        return tbSdValueDividend;
    }

    private void setTbSdValueDividend(Long tbSdValueDividend) {
        this.tbSdValueDividend = tbSdValueDividend;
        verify();
    }

    public boolean isUseOrignalNameIfZero() {
        return useOrignalNameIfZero;
    }

    private void setUseOrignalNameIfZero(boolean useOrignalNameIfZero) {
        this.useOrignalNameIfZero = useOrignalNameIfZero;
    }

    @Override
    public String parseScName(String scName, Object sdValue) {
        return parseName(scName, sdValue, scSdValueDividend);
    }

    @Override
    public String parseTbName(String tbName, Object sdValue) {
        return parseName(tbName, sdValue, tbSdValueDividend);
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
        Map<ShardRouteInfo, List<RangeShardValue>> map = new LinkedHashMap<>();
        if (scSdValueDividend == null && tbSdValueDividend == null) {
            ShardRouteInfo routeInfo = new ShardRouteInfo(scName, tbName);
            List<RangeShardValue> list = new ArrayList(1);
            list.add(new RangeShardValue(begin, end));
            map.put(routeInfo, list);
            return map;
        } else if (scSdValueDividend != null && tbSdValueDividend != null) {
            int scBegin = (int) (begin / scSdValueDividend);
            int scEnd = (int) (end / scSdValueDividend);
            int tbBegin = (int) (begin / tbSdValueDividend);
            int tbEnd = (int) (end / tbSdValueDividend);
            for (int i = scBegin; i <= scEnd; i++) {
                String scName0 = scName;
                if (i != 0 || !useOrignalNameIfZero) {
                    scName0 = scName + '_' + i;
                }
                for (int j = tbBegin; j <= tbEnd; j++) {
                    String tbName0 = tbName;
                    if (j != 0 || !useOrignalNameIfZero) {
                        tbName0 = tbName + '_' + j;
                    }
                    ShardRouteInfo routeInfo = new ShardRouteInfo(scName0, tbName0);
                    List<RangeShardValue> list = map.get(routeInfo);
                    if (list == null) {
                        list = new ArrayList<>();
                        map.put(routeInfo, list);
                    }
                    RangeShardValue rangeShardValue1 = new RangeShardValue(j * tbSdValueDividend, (j + 1)
                                                                                                  * tbSdValueDividend
                                                                                                  - 1);
                    adjustBorder(rangeShardValue1, rangeShardValue);
                    list.add(rangeShardValue1);
                }
            }
            return map;
        } else if (scSdValueDividend == null && tbSdValueDividend != null) {
            int tbBegin = (int) (begin / tbSdValueDividend);
            int tbEnd = (int) (end / tbSdValueDividend);
            for (int j = tbBegin; j <= tbEnd; j++) {
                String tbName0 = tbName;
                if (j != 0 || !useOrignalNameIfZero) {
                    tbName0 = tbName + '_' + j;
                }
                ShardRouteInfo routeInfo = new ShardRouteInfo(scName, tbName0);
                List<RangeShardValue> list = map.get(routeInfo);
                if (list == null) {
                    list = new ArrayList<>();
                    map.put(routeInfo, list);
                }
                RangeShardValue rangeShardValue1 = new RangeShardValue(j * tbSdValueDividend, (j + 1)
                                                                                              * tbSdValueDividend - 1);
                adjustBorder(rangeShardValue1, rangeShardValue);
                list.add(rangeShardValue1);
            }
            return map;
        } else {
            int scBegin = (int) (begin / scSdValueDividend);
            int scEnd = (int) (end / scSdValueDividend);
            for (int j = scBegin; j <= scEnd; j++) {
                String scName0 = scName;
                if (j != 0 || !useOrignalNameIfZero) {
                    scName0 = scName + '_' + j;
                }
                ShardRouteInfo routeInfo = new ShardRouteInfo(scName0, tbName);
                List<RangeShardValue> list = map.get(routeInfo);
                if (list == null) {
                    list = new ArrayList<>();
                    map.put(routeInfo, list);
                }
                RangeShardValue rangeShardValue1 = new RangeShardValue(j * scSdValueDividend, (j + 1)
                                                                                              * scSdValueDividend - 1);
                adjustBorder(rangeShardValue1, rangeShardValue);
                list.add(rangeShardValue1);
            }
            return map;
        }
    }

    private void adjustBorder(RangeShardValue currentRangeShardValue, RangeShardValue originalRangeShardValue) {
        if (currentRangeShardValue.getBegin() < originalRangeShardValue.getBegin()) {
            currentRangeShardValue.setBegin(originalRangeShardValue.getBegin());
        }
        if (currentRangeShardValue.getEnd() > originalRangeShardValue.getEnd()) {
            currentRangeShardValue.setEnd(originalRangeShardValue.getEnd());
        }
    }

    protected String parseName(String name, Object sdValue, Long dividend) {
        if (dividend == null) {
            return name;
        }
        if (sdValue == null) {
            throw new NullPointerException("sdValue can't be null");
        }
        if (sdValue instanceof Number) {
            long l = ((Number) sdValue).longValue();
            return parseName0(name, l, dividend);
        } else if (sdValue instanceof RangeShardValue) {
            Long begin = ((RangeShardValue) sdValue).getBegin();
            Long end = ((RangeShardValue) sdValue).getEnd();
            if (begin == null || end == null) {
                throw new IllegalArgumentException("rangeShardValue.begin and rangeShardValue.end can't be null");
            }
            if (begin > end) {
                throw new IllegalArgumentException("rangeShardValue.end must be greater than rangeShardValue.begin");
            }
            Long l = begin / dividend;
            Long l1 = end / dividend;
            if (l != l1) {
                String prefix = name + '_';
                throw new CrossTableException(prefix + l + ", " + prefix + l1);
            } else {
                if (l == 0 && useOrignalNameIfZero) {
                    return name;
                } else {
                    return new StringBuilder(name).append('_').append(l).toString();
                }
            }
        } else if (sdValue instanceof String) {
            Long l = Long.valueOf((String) sdValue);
            return parseName0(name, l, dividend);
        } else {
            throw new UnsupportedShardValueTypeException(sdValue.getClass().toString());
        }
    }

    private String parseName0(String name, Long l, Long dividend) {
        Long a = l / dividend;
        if (a == 0 && useOrignalNameIfZero) {
            return name;
        } else {
            return new StringBuilder(name).append('_').append(a).toString();
        }
    }

}
