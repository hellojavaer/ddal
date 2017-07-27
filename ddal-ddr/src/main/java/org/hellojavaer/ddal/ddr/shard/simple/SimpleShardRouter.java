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
import org.hellojavaer.ddal.ddr.shard.exception.*;
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
    private Map<String, List>                                    routeInfoMap      = new HashMap<String, List>();
    private Map<String, Set<String>>                             routedTables      = new HashMap<>();

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

    public void setRouteRuleBindings(List<SimpleShardRouteRuleBinding> bindings) {
        Map<String, InnerSimpleShardRouteRuleBindingWrapper> cache = new HashMap<String, InnerSimpleShardRouteRuleBindingWrapper>();
        Map<String, List> routeInfoMap = new HashMap<String, List>();
        Map<String, Set<String>> routedTables = new HashMap<>();
        if (bindings != null && !bindings.isEmpty()) {
            for (SimpleShardRouteRuleBinding binding : bindings) {
                // can't be null
                final String scName = DDRStringUtils.toLowerCase(binding.getScName());
                final String tbName = DDRStringUtils.toLowerCase(binding.getTbName());
                // can be null
                final String sdKey = DDRStringUtils.toLowerCase(binding.getSdKey());
                final String sdValues = DDRStringUtils.trimToNull(binding.getSdValues());

                if (scName == null) {
                    throw new IllegalArgumentException("'scName' can't be empty");
                }
                if (tbName == null) {
                    throw new IllegalArgumentException("'tbName' can't be empty");
                }
                StringBuilder sb = new StringBuilder();
                sb.append(scName).append('.').append(tbName);
                putToCache(cache, sb.toString(), binding, true);

                final SimpleShardRouteRuleBinding b0 = new SimpleShardRouteRuleBinding();
                b0.setScName(scName);
                b0.setTbName(tbName);
                b0.setSdKey(sdKey);
                b0.setRule(binding.getRule());
                putToCache(cache, tbName, b0, false);

                final Set<RouteInfo> routeInfos = new LinkedHashSet<RouteInfo>();
                if (sdValues != null) {
                    RangeExpression.parse(sdValues, new RangeItemVisitor() {

                        @Override
                        public void visit(Object val) {
                            RouteInfo routeInfo = getRouteInfo(b0, scName, tbName, val);
                            routeInfos.add(routeInfo);
                        }
                    });
                }
                String key = buildQueryKey(scName, tbName);
                if (routeInfoMap.containsKey(key)) {
                    throw new IllegalArgumentException("Duplicate route config for table '" + key + "'");
                } else {
                    Set<String> tables = routedTables.get(scName);
                    if (tables == null) {
                        tables = new LinkedHashSet<>();
                        routedTables.put(scName, tables);
                    }
                    tables.add(tbName);
                    routeInfoMap.put(key, new ArrayList(routeInfos));
                }
            }
        }
        this.routeRuleBindings = bindings;
        this.cache = cache;
        this.routeInfoMap = routeInfoMap;
        this.routedTables = routedTables;
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
    public RouteInfo getRouteInfo(String scName, String tbName, Object sdValue) throws ShardValueNotFoundException,
                                                                                       ShardRouteException {
        scName = DDRStringUtils.toLowerCase(scName);
        tbName = DDRStringUtils.toLowerCase(tbName);
        InnerSimpleShardRouteRuleBindingWrapper bindingWrapper = getBinding(scName, tbName);
        SimpleShardRouteRuleBinding binding = bindingWrapper.getRuleBinding();
        if (binding == null) {
            return null;
        } else {// 必须使用 binding 中的 scName,因为sql中的scName可能为空
            RouteInfo info = getRouteInfo(binding, binding.getScName(), binding.getTbName(), sdValue);
            return info;
        }
    }

    @Override
    public List<RouteInfo> getRouteInfos(String scName, String tbName) throws ShardValueNotFoundException,
                                                                              ShardRouteException {
        return routeInfoMap.get(buildQueryKey(scName, tbName));
    }

    @Override
    public Map<String, Set<String>> getRoutedTables() {
        return routedTables;
    }

    private static String buildQueryKey(String scName, String tbName) {
        StringBuilder sb = new StringBuilder();
        scName = DDRStringUtils.toLowerCase(scName);
        if (scName != null) {
            sb.append(scName);
            sb.append('.');
        }
        sb.append(DDRStringUtils.toLowerCase(tbName));
        return sb.toString();
    }

    private RouteInfo getRouteInfo(SimpleShardRouteRuleBinding binding, String scName, String tbName, Object sdValue)
            throws ShardRouteException,
                   ShardValueNotFoundException {
        ShardRouteRule rule = binding.getRule();
        if (rule == null) {// 未配置rule,参数sdKey 和 sdValue都无效
            RouteInfo info = new RouteInfo();
            info.setScName(scName);
            info.setTbName(tbName);
            return info;
        } else {// 配置了rule
            if (sdValue == null) {
                Object obj = ShardRouteContext.getRouteInfo(scName, tbName);
                if (obj == null) {
                    throw new ShardValueNotFoundException("shard value is not found for scName:" + scName + ", tbName:"
                                                          + tbName + ",routeRule:" + rule);
                }
                if (binding.getSdKey() != null && !(obj instanceof RouteInfo)) {
                    throw new IllegalShardValueException(
                                                         "when 'sdKey' is configed in route rule for table '"
                                                                 + scName
                                                                 + "."
                                                                 + tbName
                                                                 + "', "
                                                                 + ", the type of 'sdValue' can only be 'RouteInfo'. but current 'sdValue' is "
                                                                 + obj.getClass() + "(" + obj + ")");
                }
                if (obj instanceof RouteInfo) {
                    return (RouteInfo) obj;
                } else {
                    return getRouteInfoByRouteRule(rule, scName, tbName, obj);
                }
            } else {
                return getRouteInfoByRouteRule(rule, scName, tbName, sdValue);
            }
        }
    }

    private RouteInfo getRouteInfoByRouteRule(ShardRouteRule rule, String scName, String tbName, Object sdValue) {
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
            throw new ShardRouteException("'scName':" + scName + ",'tbName':" + tbName + ",'sdName':" + sdValue
                                          + ",routeRule:" + rule, e);
        }
    }
}
