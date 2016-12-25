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
import org.hellojavaer.ddr.core.expression.range.RangeExpression;
import org.hellojavaer.ddr.core.expression.range.RangeItemVisitor;
import org.hellojavaer.ddr.core.sharding.*;
import org.hellojavaer.ddr.core.sharding.exception.AmbiguousDataSourceBindingException;
import org.hellojavaer.ddr.core.sharding.exception.ConflictingDataSourceBindingException;
import org.hellojavaer.ddr.core.sharding.exception.NoDataSourceBindingException;
import org.hellojavaer.ddr.core.utils.DDRStringUtils;

import java.util.*;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 14/11/2016.
 */
public class SimpleShardingRouter implements ShardingRouter {

    private final Log                                               logger = LogFactory.getLog(getClass());
    private List<SimpleShardingRouteRuleBinding>                    routeRuleBindings;
    private Map<String, InnerSimpleShardingRouteRuleBindingWrapper> cache  = Collections.EMPTY_MAP;

    public List<SimpleShardingRouteRuleBinding> getRouteRuleBindings() {
        return routeRuleBindings;
    }

    protected class InnerSimpleShardingRouteRuleBindingWrapper {

        private List<String>                   conflictSchemas = new ArrayList<String>();
        private SimpleShardingRouteRuleBinding ruleBinding;
        private RouteConfig                    routeInfo;

        public List<String> getConflictSchemas() {
            return conflictSchemas;
        }

        public void setConflictSchemas(List<String> conflictSchemas) {
            this.conflictSchemas = conflictSchemas;
        }

        public SimpleShardingRouteRuleBinding getRuleBinding() {
            return ruleBinding;
        }

        public void setRuleBinding(SimpleShardingRouteRuleBinding ruleBinding) {
            this.ruleBinding = ruleBinding;
        }

        public RouteConfig getRouteInfo() {
            return routeInfo;
        }

        public void setRouteInfo(RouteConfig routeInfo) {
            this.routeInfo = routeInfo;
        }
    }

    public void setRouteRuleBindings(List<SimpleShardingRouteRuleBinding> routeRuleBindings) {
        this.routeRuleBindings = routeRuleBindings;
        refreshCache(routeRuleBindings);
    }

    private void refreshCache(List<SimpleShardingRouteRuleBinding> bindings) {
        Map<String, InnerSimpleShardingRouteRuleBindingWrapper> cache0 = new HashMap<String, InnerSimpleShardingRouteRuleBindingWrapper>();
        if (bindings != null && !bindings.isEmpty()) {
            for (SimpleShardingRouteRuleBinding binding : bindings) {
                // can't be null
                final String scName = DDRStringUtils.toLowerCase(binding.getScName());
                final String tbName = DDRStringUtils.toLowerCase(binding.getTbName());
                // can be null
                final String sdName = DDRStringUtils.toLowerCase(binding.getSdName());
                final String sdScanValues = DDRStringUtils.trim(binding.getSdScanValues());
                final String sdScanValueType = DDRStringUtils.trim(binding.getSdScanValueType());

                if (scName == null) {
                    throw new IllegalArgumentException("'scName' can't be empty");
                }
                if (tbName == null) {
                    throw new IllegalArgumentException("'tbName' can't be empty");
                }
                StringBuilder sb = new StringBuilder();
                sb.append(scName).append('.').append(tbName);
                putToCache(cache0, sb.toString(), binding, true);

                final SimpleShardingRouteRuleBinding b0 = new SimpleShardingRouteRuleBinding();
                b0.setScName(scName);
                b0.setTbName(tbName);
                b0.setSdName(sdName);
                b0.setRule(binding.getRule());
                putToCache(cache0, tbName, b0, false);

                final List<RouteInfo> shardingInfos = new ArrayList<RouteInfo>();
                if (sdScanValues != null) {
                    if (sdScanValueType == null) {
                        throw new IllegalArgumentException(
                                                           "'sdScanValueType' can't be empty when 'sdScanValues' is set value");
                    }
                    RangeExpression.parse(sdScanValues, new RangeItemVisitor() {

                        @Override
                        public void visit(String val) {
                            Object v = val;
                            if (SimpleShardingRouteRuleBinding.VALUE_TYPE_OF_NUMBER.equals(sdScanValueType)) {
                                v = Long.valueOf(val);
                            } else if (SimpleShardingRouteRuleBinding.VALUE_TYPE_OF_STRING.equals(sdScanValueType)) {
                                // ok
                            } else {
                                throw new IllegalArgumentException("Unknown 'sdScanValueType':" + sdScanValueType);
                            }
                            RouteInfo routeInfo = getRouteInfo(b0.getRule(), scName, tbName, v);
                            shardingInfos.add(routeInfo);
                        }
                    });
                }
                ShardingRouteHelper.setConfiguredRouteInfos(scName, tbName, shardingInfos);
            }
        }
        this.cache = cache0;
    }

    private void putToCache(Map<String, InnerSimpleShardingRouteRuleBindingWrapper> cache, String key,
                            SimpleShardingRouteRuleBinding ruleBinding, boolean checkConflict) {
        InnerSimpleShardingRouteRuleBindingWrapper ruleBindingWrapper = cache.get(key);
        if (checkConflict && ruleBindingWrapper != null) {
            throw new ConflictingDataSourceBindingException("Conflict route binding for scName:"
                                                            + ruleBinding.getScName() + ", tbName:"
                                                            + ruleBinding.getTbName() + ", sdName:"
                                                            + ruleBinding.getSdName());
        }
        if (ruleBindingWrapper == null) {
            ruleBindingWrapper = new InnerSimpleShardingRouteRuleBindingWrapper();
            ruleBindingWrapper.setRuleBinding(ruleBinding);
            ruleBindingWrapper.setRouteInfo(new RouteConfig(ruleBinding.getScName(), ruleBinding.getTbName(),
                                                            ruleBinding.getSdName()));
            cache.put(key, ruleBindingWrapper);
        }
        ruleBindingWrapper.getConflictSchemas().add(ruleBinding.getScName());
    }

    private InnerSimpleShardingRouteRuleBindingWrapper getBinding(String scName, String tbName) {
        if (tbName == null) {
            throw new IllegalArgumentException("'tbName' can't be empty");
        }
        String queryKey = tbName;
        if (scName != null) {
            queryKey = new StringBuilder().append(scName).append('.').append(tbName).toString();
        }
        InnerSimpleShardingRouteRuleBindingWrapper ruleBindingWrapper = cache.get(queryKey);
        if (ruleBindingWrapper == null) {
            return null;
        } else if (ruleBindingWrapper.getConflictSchemas().size() > 1) {
            throw new AmbiguousDataSourceBindingException("[getBinding] Binding for 'scName':" + scName + ", 'tbName':"
                                                          + tbName + " is ambiguous");
        } else {
            return ruleBindingWrapper;
        }
    }

    @Override
    public RouteConfig getRouteConfig(ShardingRouteParamContext context, String scName, String tbName) {
        scName = DDRStringUtils.toLowerCase(scName);
        tbName = DDRStringUtils.toLowerCase(tbName);
        InnerSimpleShardingRouteRuleBindingWrapper bindingWrapper = getBinding(scName, tbName);
        if (bindingWrapper == null) {
            throw new NoDataSourceBindingException("No route rule binding for 'scName':" + scName + " and 'tbName':"
                                                   + tbName);
        } else {
            return bindingWrapper.getRouteInfo();
        }
    }

    @Override
    public RouteInfo route(ShardingRouteParamContext context, String scName, String tbName, Object sdValue) {
        scName = DDRStringUtils.toLowerCase(scName);
        tbName = DDRStringUtils.toLowerCase(tbName);
        InnerSimpleShardingRouteRuleBindingWrapper bindingWrapper = getBinding(scName, tbName);
        SimpleShardingRouteRuleBinding binding = bindingWrapper.getRuleBinding();
        if (binding == null) {
            throw new NoDataSourceBindingException("No route rule binding for 'scName':" + scName + " 'tbName':" + tbName);
        } else {// 必须使用 binding 中的 scName,因为sql中的scName可能为空
            RouteInfo info = getRouteInfo(binding.getRule(), binding.getScName(), binding.getTbName(), sdValue);
            return info;
        }
    }

    private RouteInfo getRouteInfo(SimpleShardingRouterRule rule, String scName, String tbName, Object sdValue) {
        if (sdValue == null || rule == null) {
            if (rule == null && rule == null) {
                RouteInfo info = new RouteInfo();
                info.setScName(scName);
                info.setTbName(tbName);
                return info;
            } else {
                return null;
            }
        } else {
            RouteInfo info = new RouteInfo();
            Object $0 = rule.parseScRoute(scName, sdValue);
            String sc = rule.parseScFormat(scName, $0);
            Object $0_ = rule.parseTbRoute(tbName, sdValue);
            String tb = rule.parseTbFormat(tbName, $0_);
            if (tb == null) {
                throw new IllegalArgumentException("'tbName' can't be null");
            }
            info.setScName(sc);
            info.setTbName(tb);
            return info;
        }
    }

}
