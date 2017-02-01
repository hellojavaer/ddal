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

import org.hellojavaer.ddal.ddr.expression.range.RangeExpression;
import org.hellojavaer.ddal.ddr.expression.range.RangeItemVisitor;
import org.hellojavaer.ddal.ddr.shard.*;
import org.hellojavaer.ddal.ddr.shard.exception.AmbiguousRouteRuleBindingException;
import org.hellojavaer.ddal.ddr.shard.exception.DuplicateRouteRuleBindingException;
import org.hellojavaer.ddal.ddr.shard.exception.ShardRoutingException;
import org.hellojavaer.ddal.ddr.shard.exception.ShardValueNotFoundException;
import org.hellojavaer.ddal.ddr.utils.DDRStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 14/11/2016.
 */
public class SimpleShardRouter implements ShardRouter {

    private Logger                                               logger            = LoggerFactory.getLogger(getClass());
    private List<SimpleShardRouteRuleBinding>                    routeRuleBindings = null;
    private Map<String, InnerSimpleShardRouteRuleBindingWrapper> cache             = Collections.EMPTY_MAP;

    public List<SimpleShardRouteRuleBinding> getRouteRuleBindings() {
        return routeRuleBindings;
    }

    protected class InnerSimpleShardRouteRuleBindingWrapper {

        private List<String>                conflictSchemas = new ArrayList<String>();
        private SimpleShardRouteRuleBinding ruleBinding;
        private RouteConfig                 routeConfig;

        public List<String> getConflictSchemas() {
            return conflictSchemas;
        }

        public void setConflictSchemas(List<String> conflictSchemas) {
            this.conflictSchemas = conflictSchemas;
        }

        public SimpleShardRouteRuleBinding getRuleBinding() {
            return ruleBinding;
        }

        public void setRuleBinding(SimpleShardRouteRuleBinding ruleBinding) {
            this.ruleBinding = ruleBinding;
        }

        public RouteConfig getRouteConfig() {
            return routeConfig;
        }

        public void setRouteConfig(RouteConfig routeConfig) {
            this.routeConfig = routeConfig;
        }
    }

    public void setRouteRuleBindings(List<SimpleShardRouteRuleBinding> routeRuleBindings) {
        this.routeRuleBindings = routeRuleBindings;
        refreshCache(routeRuleBindings);
    }

    private void refreshCache(List<SimpleShardRouteRuleBinding> bindings) {
        Map<String, InnerSimpleShardRouteRuleBindingWrapper> cache0 = new HashMap<String, InnerSimpleShardRouteRuleBindingWrapper>();
        if (bindings != null && !bindings.isEmpty()) {
            for (SimpleShardRouteRuleBinding binding : bindings) {
                // can't be null
                final String scName = DDRStringUtils.toLowerCase(binding.getScName());
                final String tbName = DDRStringUtils.toLowerCase(binding.getTbName());
                // can be null
                final String sdKey = DDRStringUtils.toLowerCase(binding.getSdKey());
                final String sdValues = DDRStringUtils.trimToNull(binding.getSdValues());
                final String sdValueType = DDRStringUtils.trimToNull(binding.getSdValueType());

                if (scName == null) {
                    throw new IllegalArgumentException("'scName' can't be empty");
                }
                if (tbName == null) {
                    throw new IllegalArgumentException("'tbName' can't be empty");
                }
                StringBuilder sb = new StringBuilder();
                sb.append(scName).append('.').append(tbName);
                putToCache(cache0, sb.toString(), binding, true);

                final SimpleShardRouteRuleBinding b0 = new SimpleShardRouteRuleBinding();
                b0.setScName(scName);
                b0.setTbName(tbName);
                b0.setSdKey(sdKey);
                b0.setRule(binding.getRule());
                putToCache(cache0, tbName, b0, false);

                final List<RouteInfo> routeInfos = new ArrayList<RouteInfo>();
                if (sdValues != null) {
                    if (sdValueType == null) {
                        throw new IllegalArgumentException("'sdValueType' can't be empty when 'sdValues' is set value");
                    }
                    RangeExpression.parse(sdValues, new RangeItemVisitor() {

                        @Override
                        public void visit(String val) {
                            Object v = val;
                            if (SimpleShardRouteRuleBinding.VALUE_TYPE_OF_NUMBER.equals(sdValueType)) {
                                v = Long.valueOf(val);
                            } else if (SimpleShardRouteRuleBinding.VALUE_TYPE_OF_STRING.equals(sdValueType)) {
                                // ok
                            } else {
                                throw new IllegalArgumentException("Unknown 'sdValueType':" + sdValueType);
                            }
                            RouteInfo routeInfo = getRouteInfo(b0.getRule(), scName, tbName, v);
                            routeInfos.add(routeInfo);
                        }
                    });
                }
                ShardRouteHelper.setConfiguredRouteInfos(scName, tbName, routeInfos);
            }
        }
        this.cache = cache0;
    }

    private void putToCache(Map<String, InnerSimpleShardRouteRuleBindingWrapper> cache, String key,
                            SimpleShardRouteRuleBinding ruleBinding, boolean checkConflict) {
        InnerSimpleShardRouteRuleBindingWrapper ruleBindingWrapper = cache.get(key);
        if (checkConflict && ruleBindingWrapper != null) {
            throw new DuplicateRouteRuleBindingException("Duplicate route rule binding for scName:"
                                                         + ruleBinding.getScName() + ", tbName:"
                                                         + ruleBinding.getTbName());
        }
        if (ruleBindingWrapper == null) {
            ruleBindingWrapper = new InnerSimpleShardRouteRuleBindingWrapper();
            ruleBindingWrapper.setRuleBinding(ruleBinding);
            ruleBindingWrapper.setRouteConfig(new RouteConfig(ruleBinding.getScName(), ruleBinding.getTbName(),
                                                              ruleBinding.getSdKey()));
            cache.put(key, ruleBindingWrapper);
        }
        ruleBindingWrapper.getConflictSchemas().add(ruleBinding.getScName());
    }

    private InnerSimpleShardRouteRuleBindingWrapper getBinding(String scName, String tbName) {
        if (tbName == null) {
            throw new IllegalArgumentException("'tbName' can't be empty");
        }
        String queryKey = tbName;
        if (scName != null) {
            queryKey = new StringBuilder().append(scName).append('.').append(tbName).toString();
        }
        InnerSimpleShardRouteRuleBindingWrapper ruleBindingWrapper = cache.get(queryKey);
        if (ruleBindingWrapper == null) {
            return null;
        } else if (ruleBindingWrapper.getConflictSchemas().size() > 1) {
            throw new AmbiguousRouteRuleBindingException("route rule binding for 'scName':" + scName + ", 'tbName':"
                                                         + tbName + " is ambiguous");
        } else {
            return ruleBindingWrapper;
        }
    }

    @Override
    public RouteConfig getRouteConfig(String scName, String tbName) {
        scName = DDRStringUtils.toLowerCase(scName);
        tbName = DDRStringUtils.toLowerCase(tbName);
        InnerSimpleShardRouteRuleBindingWrapper bindingWrapper = getBinding(scName, tbName);
        if (bindingWrapper == null) {
            return null;
        } else {
            return bindingWrapper.getRouteConfig();
        }
    }

    @Override
    public RouteInfo route(String scName, String tbName, Object sdValue) throws ShardValueNotFoundException,
                                                                        ShardRoutingException {
        scName = DDRStringUtils.toLowerCase(scName);
        tbName = DDRStringUtils.toLowerCase(tbName);
        InnerSimpleShardRouteRuleBindingWrapper bindingWrapper = getBinding(scName, tbName);
        SimpleShardRouteRuleBinding binding = bindingWrapper.getRuleBinding();
        if (binding == null) {
            return null;
        } else {// 必须使用 binding 中的 scName,因为sql中的scName可能为空
            RouteInfo info = getRouteInfo(binding.getRule(), binding.getScName(), binding.getTbName(), sdValue);
            return info;
        }
    }

    private RouteInfo getRouteInfo(ShardRouteRule rule, String scName, String tbName, Object sdValue)
                                                                                                     throws ShardRoutingException,
                                                                                                     ShardValueNotFoundException {
        if (rule == null) {// 未配置rule,参数sdKey 和 sdValue都无效
            RouteInfo info = new RouteInfo();
            info.setScName(scName);
            info.setTbName(tbName);
            return info;
        } else {// 配置了rule
            if (sdValue == null) {
                Object obj = ShardRouteContext.getRouteInfo(scName, tbName);
                if (obj != null) {
                    if (obj instanceof RouteInfo) {
                        return (RouteInfo) obj;
                    } else {
                        return getRouteInfo(rule, scName, tbName, obj);
                    }
                } else {
                    throw new ShardValueNotFoundException("shard value is not found for 'scName':" + scName
                                                          + ",'tbName':" + tbName + ",routeRule:" + rule);
                }
            }
            //
            try {
                RouteInfo info = new RouteInfo();
                ShardRouteRuleContext context = new ShardRouteRuleContext();
                context.setScName(scName);
                context.setTbName(tbName);
                context.setSdValue(sdValue);
                // throws exception
                String sc = rule.parseScName(context);
                // throws exception
                String tb = rule.parseTbName(context);

                info.setScName(sc);
                info.setTbName(tb);
                return info;
            } catch (Throwable e) {
                throw new ShardRoutingException("'scName':" + scName + ",'tbName':" + tbName + ",'sdName':" + sdValue
                                                + ",routeRule:" + rule, e);
            }
        }
    }
}
