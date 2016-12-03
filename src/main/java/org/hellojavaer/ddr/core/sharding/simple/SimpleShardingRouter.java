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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hellojavaer.ddr.core.exception.DDRException;
import org.hellojavaer.ddr.core.expression.range.RangeExpression;
import org.hellojavaer.ddr.core.expression.range.RangeItemVisitor;
import org.hellojavaer.ddr.core.sharding.*;
import org.hellojavaer.ddr.core.utils.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">zoukaiming[邹凯明]</a>,created on 14/11/2016.
 */
public class SimpleShardingRouter implements ShardingRouter {

    private final Log                                   logger = LogFactory.getLog(getClass());
    private List<SimpleShardingRouteRuleBinding>        routeRuleBindings;
    private Map<String, SimpleShardingRouteRuleBinding> cache  = Collections.EMPTY_MAP;

    public List<SimpleShardingRouteRuleBinding> getRouteRuleBindings() {
        return routeRuleBindings;
    }

    public void setRouteRuleBindings(List<SimpleShardingRouteRuleBinding> routeRuleBindings) {
        this.routeRuleBindings = routeRuleBindings;
        refreshCache(routeRuleBindings);
    }

    private void refreshCache(List<SimpleShardingRouteRuleBinding> bindings) {
        ConcurrentHashMap<String, SimpleShardingRouteRuleBinding> temp = new ConcurrentHashMap<String, SimpleShardingRouteRuleBinding>();
        if (bindings != null && !bindings.isEmpty()) {
            for (SimpleShardingRouteRuleBinding binding : bindings) {
                if (binding.getRule() == null) {
                    throw new DDRException("binding for scName:" + binding.getScName() + " tbName:"
                                           + binding.getTbName() + " sdName:" + binding.getSdName() + " can't be null");
                }
                final String scName = filter(binding.getScName());
                final String tbName = filter(binding.getTbName());
                final String sdName = filter(binding.getSdName());
                final String sdScanValues = StringUtils.trim(binding.getSdScanValues());

                if (tbName == null) {
                    throw new DDRException("tbName can't be empty");
                }
                if (scName != null) {
                    StringBuilder sb = new StringBuilder();
                    sb.append(scName).append('.').append(tbName);
                    putToCache(temp, sb.toString(), binding);
                }
                final SimpleShardingRouteRuleBinding b0 = new SimpleShardingRouteRuleBinding();
                b0.setScName(scName);
                b0.setTbName(tbName);
                b0.setSdName(sdName);
                b0.setRule(binding.getRule());

                final List<ShardingInfo> shardingInfos = new ArrayList<ShardingInfo>();
                if (sdScanValues != null) {
                    RangeExpression.parse(sdScanValues, new RangeItemVisitor() {

                        @Override
                        public void visit(String val) {
                            try {
                                Long v = Long.valueOf(val);
                                ShardingInfo shardingInfo = getShardingInfo(b0.getRule(), scName, tbName, v);
                                shardingInfos.add(shardingInfo);
                            } catch (RuntimeException e) {
                                throw e;// TODO
                            }
                        }
                    });
                }
                ShardingRouteHelper.setConfigedShardingInfos(scName, tbName, shardingInfos);
                putToCache(temp, tbName, b0);
            }
        }
        this.cache = temp;
    }

    private void putToCache(ConcurrentHashMap<String, SimpleShardingRouteRuleBinding> cache, String key,
                            SimpleShardingRouteRuleBinding val) {
        if (cache.putIfAbsent(key, val) != null) {
            throw new DDRException("conflict binding for tbName:" + val.getScName() + " tbName:" + val.getTbName()
                                   + " sdName:" + val.getSdName());
        }
    }

    private String filter(String str) {
        if (str == null) {
            return null;
        } else {
            str = str.trim();
            if (str.length() == 0) {
                return null;
            } else {
                return str.toLowerCase();
            }
        }
    }

    @Override
    public void beginExecution(ShardingRouteParamContext context) {

    }

    @Override
    public void endExecution(ShardingRouteParamContext context) {

    }

    @Override
    public void beginStatement(ShardingRouteParamContext context, int type) {
    }

    @Override
    public void endStatement(ShardingRouteParamContext context) {
    }

    @Override
    public void beginSubSelect(ShardingRouteParamContext context) {

    }

    @Override
    public void endSubSelect(ShardingRouteParamContext context) {

    }

    @Override
    public boolean isRoute(ShardingRouteParamContext context, String scName, String tbName) {
        scName = filter(scName);
        tbName = filter(tbName);
        if (getBinding(scName, tbName) == null) {
            return false;
        } else {
            return true;
        }
    }

    private SimpleShardingRouteRuleBinding getBinding(String scName, String tbName) {
        if (tbName == null) {
            throw new DDRException("tbName can't be empty");
        }
        if (scName == null) {
            return cache.get(tbName);
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append(scName).append('.').append(tbName);
            SimpleShardingRouteRuleBinding binding = cache.get(sb.toString());// 全名称匹配
            if (binding == null) {
                binding = cache.get(tbName);
                if (binding != null && //
                    !(stringEquals(binding.getScName(), scName) && stringEquals(binding.getTbName(), tbName))//
                ) {//
                    binding = null;
                }
            }
            return binding;
        }
    }

    private boolean stringEquals(String s0, String s1) {
        if (s0 == null) {
            if (s1 == null) {
                return true;
            } else {
                return false;
            }
        } else {
            return s0.equals(s1);
        }
    }

    @Override
    public String getRouteColName(ShardingRouteParamContext context, String scName, String tbName) {
        scName = filter(scName);
        tbName = filter(tbName);
        SimpleShardingRouteRuleBinding binding = getBinding(scName, tbName);
        if (binding == null) {
            throw new DDRException("");
        } else {
            return binding.getSdName();
        }
    }

    @Override
    public ShardingInfo route(ShardingRouteParamContext context, String scName, String tbName, Long sdValue) {
        scName = filter(scName);
        tbName = filter(tbName);
        SimpleShardingRouteRuleBinding binding = getBinding(scName, tbName);
        if (binding == null) {
            throw new DDRException("no binding for  scName:" + scName + " tbName:" + tbName);
        } else {
            if (scName == null) {
                scName = binding.getScName();
            }
            if (binding.getSdName() == null && sdValue != null) {
                logger.warn("scName:" + scName + " tbName:" + tbName + " sdName:null , sdValue expect null but "
                            + sdValue);
            }
            if (sdValue == null) {
                Object routeValue = ShardingRouteContext.getRoute(scName, tbName);
                if (routeValue == null) {
                    throw new DDRException("not route information for [scName:" + scName + ",tbName:" + tbName
                                           + ",sdName:" + binding.getSdName() + "]");
                }
                if (routeValue instanceof ShardingInfo) {
                    return (ShardingInfo) routeValue;
                } else if (routeValue instanceof Long) {
                    sdValue = (Long) routeValue;
                } else {
                    throw new DDRException("unexpected type[" + routeValue.getClass() + "] for [scName:" + scName
                                           + ",tbName:" + tbName + ",sdName:" + binding.getSdName()
                                           + "] in ShardingContext");
                }
            }
            if (sdValue == null) {
                throw new DDRException("not route information for [scName:" + scName + ",tbName:" + tbName + ",sdName:"
                                       + binding.getSdName() + "]");
            }
            ShardingInfo info = getShardingInfo(binding.getRule(), scName, tbName, sdValue);
            return info;
        }
    }

    private ShardingInfo getShardingInfo(SimpleShardingRouterRule rule, String scName, String tbName, Long sdValue) {
        Object $0 = rule.parseScRoute(scName, sdValue);
        String sc = rule.parseScFormat(scName, $0);
        Object $0_ = rule.parseTbRoute(tbName, sdValue);
        String tb = rule.parseTbFormat(tbName, $0_);
        if (tb == null) {
            throw new DDRException("");
        }
        ShardingInfo info = new ShardingInfo();
        info.setScName(sc);
        info.setTbName(tb);
        return info;
    }

}
