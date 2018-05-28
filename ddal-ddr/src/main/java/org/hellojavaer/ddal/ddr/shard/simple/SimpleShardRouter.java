/*
 * Copyright 2016-2018 the original author or authors.
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

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.Weighers;
import org.hellojavaer.ddal.ddr.expression.range.RangeExpressionItemVisitor;
import org.hellojavaer.ddal.ddr.expression.range.RangeExpressionParser;
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

    private Logger                                               logger                        = LoggerFactory.getLogger(getClass());

    // schema+table级路由配置
    private List<SimpleShardRouteRuleBinding>                    routeRuleBindings             = null;
    private Map<String, InnerSimpleShardRouteRuleBindingWrapper> routeInfoCache                = Collections.EMPTY_MAP;
    private Map<String, List<ShardRouteInfo>>                    routeInfosCache               = new HashMap<>();
    private Map<String, Set<String>>                             routedTables                  = new HashMap<>();

    // schema级路由配置
    private Map<String, SimpleShardRouteRuleBinding>             schemaBindingMap              = new HashMap<>();
    private SimpleShardRouteRuleBinding                          defaultSchemaBinding          = null;
    private Map<String, List<ShardRouteInfo>>                    routeInfosCacheForSchemaLevel = new ConcurrentLinkedHashMap.Builder<String, List<ShardRouteInfo>>()//
                                                                                               .maximumWeightedCapacity(10000)//
                                                                                               .weigher(Weighers.singleton())//
                                                                                               .build();

    private SimpleShardRouter() {
    }

    public SimpleShardRouter(List<SimpleShardRouteRuleBinding> routeRuleBindings) {
        setRouteRuleBindings(routeRuleBindings);
    }

    public List<SimpleShardRouteRuleBinding> getRouteRuleBindings() {
        return routeRuleBindings;
    }

    public void setRouteRuleBindings(List<SimpleShardRouteRuleBinding> bindings) {
        Map<String, InnerSimpleShardRouteRuleBindingWrapper> cache = new HashMap<>();
        Map<String, List<ShardRouteInfo>> routeInfoMap = new HashMap<>();
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
                if (tbName != null) {
                    final SimpleShardRouteRuleBinding b0 = new SimpleShardRouteRuleBinding();
                    b0.setScName(scName);
                    b0.setTbName(tbName);
                    b0.setSdKey(sdKey);
                    b0.setRule(binding.getRule());
                    StringBuilder sb = new StringBuilder();
                    sb.append(scName).append('.').append(tbName);
                    putToCache(cache, sb.toString(), b0, true);
                    putToCache(cache, tbName, b0, false);

                    // 构建 逻辑sc+逻辑tb下包含的所有物理表信息
                    final Set<ShardRouteInfo> routeInfos = new LinkedHashSet<>();
                    if (binding.getRule() == null) {// 如果路由规则为空,则使用原表名
                        ShardRouteInfo routeInfo = new ShardRouteInfo();
                        routeInfo.setScName(scName);
                        routeInfo.setTbName(tbName);
                        routeInfos.add(routeInfo);
                    } else {
                        if (sdValues != null) {
                            new RangeExpressionParser(sdValues).visit(new RangeExpressionItemVisitor() {

                                @Override
                                public void visit(Object val) {
                                    ShardRouteInfo routeInfo = getRouteInfo(b0, scName, tbName, val);
                                    routeInfos.add(routeInfo);
                                }
                            });
                        }
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
                } else {// 配置schema级别路由规则
                    if (defaultSchemaBinding == null) {// 使用列表中的第一个作为默认配置
                        defaultSchemaBinding = binding;
                    }
                    if (schemaBindingMap.put(scName, binding) != null) {
                        throw new DuplicateRouteRuleBindingException("Duplicate route rule binding for scName:"
                                                                     + scName);
                    }
                }
            }
        }
        this.routeRuleBindings = bindings;
        this.routeInfoCache = cache;
        this.routeInfosCache = routeInfoMap;
        this.routedTables = routedTables;
    }

    protected class InnerSimpleShardRouteRuleBindingWrapper {

        private List<String>                conflictSchemas = new ArrayList<String>();
        private SimpleShardRouteRuleBinding ruleBinding;
        private ShardRouteConfig            routeConfig;

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

        public ShardRouteConfig getRouteConfig() {
            return routeConfig;
        }

        public void setRouteConfig(ShardRouteConfig routeConfig) {
            this.routeConfig = routeConfig;
        }
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
            ruleBindingWrapper.setRouteConfig(new ShardRouteConfig(ruleBinding.getScName(), ruleBinding.getTbName(),
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
        InnerSimpleShardRouteRuleBindingWrapper ruleBindingWrapper = routeInfoCache.get(queryKey);
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
    public ShardRouteRule getRouteRule(String scName, String tbName) {
        scName = DDRStringUtils.toLowerCase(scName);
        tbName = DDRStringUtils.toLowerCase(tbName);
        if (tbName == null) {
            throw new IllegalArgumentException("tbName can't be null");
        }
        InnerSimpleShardRouteRuleBindingWrapper bindingWrapper = getBinding(scName, tbName);
        if (bindingWrapper != null) {// 1.从schema+table级中获取
            if (bindingWrapper.getRuleBinding() != null) {
                return bindingWrapper.getRuleBinding().getRule();
            } else {
                return null;
            }
        } else {// 2.从schema级别获取
            SimpleShardRouteRuleBinding binding = getRouteRuleBindingBySchemaLevel(scName);
            if (binding != null) {
                return binding.getRule();
            } else {
                return null;
            }
        }
    }

    @Override
    public ShardRouteConfig getRouteConfig(String scName, String tbName) {
        scName = DDRStringUtils.toLowerCase(scName);
        tbName = DDRStringUtils.toLowerCase(tbName);
        if (tbName == null) {
            throw new IllegalArgumentException("tbName can't be null");
        }
        InnerSimpleShardRouteRuleBindingWrapper bindingWrapper = getBinding(scName, tbName);
        if (bindingWrapper != null) {// 1.从schema+table级中获取
            return bindingWrapper.getRouteConfig();
        } else {// 2.从schema级别获取
            SimpleShardRouteRuleBinding binding = getRouteRuleBindingBySchemaLevel(scName);
            if (binding == null) {
                return null;
            } else {
                ShardRouteConfig config = new ShardRouteConfig();
                config.setScName(binding.getScName());
                config.setTbName(tbName);// note here
                config.setSdKey(binding.getSdKey());
                return config;
            }
        }
    }

    @Override
    public ShardRouteInfo getRouteInfo(String scName, String tbName, Object sdValue)
                                                                                    throws ShardValueNotFoundException,
                                                                                    ShardRouteException {
        scName = DDRStringUtils.toLowerCase(scName);
        tbName = DDRStringUtils.toLowerCase(tbName);
        if (tbName == null) {
            throw new IllegalArgumentException("tbName can't be null");
        }
        SimpleShardRouteRuleBinding binding = null;
        InnerSimpleShardRouteRuleBindingWrapper bindingWrapper = getBinding(scName, tbName);
        if (bindingWrapper != null) {// 1.从schema+table级中获取
            binding = bindingWrapper.getRuleBinding();
        } else {// 2.从schema级获取
            binding = getRouteRuleBindingBySchemaLevel(scName);
        }
        if (binding == null) {
            return null;
        } else {// 必须使用binding中的scName,因为sql中的scName可能为空,binding.getTbName()可能为null必须使用tbName
            ShardRouteInfo info = getRouteInfo(binding, binding.getScName(), tbName, sdValue);
            return info;
        }
    }

    /**
     * 
     * @param scName can be null
     * @param tbName can't be null
     * @return if sdValues is null or scName-tbName don't hit config return null
     * @throws ShardValueNotFoundException
     * @throws ShardRouteException
     */
    @Override
    public List<ShardRouteInfo> getRouteInfos(String scName, String tbName) throws ShardValueNotFoundException,
                                                                           ShardRouteException {
        scName = DDRStringUtils.toLowerCase(scName);
        tbName = DDRStringUtils.toLowerCase(tbName);
        if (tbName == null) {
            throw new IllegalArgumentException("tbName can't be null");
        }
        List<ShardRouteInfo> result = routeInfosCache.get(buildQueryKey(scName, tbName));
        if (result != null) {
            return result;
        } else {// 查询schema配置级别
            SimpleShardRouteRuleBinding binding = getRouteRuleBindingBySchemaLevel(scName);
            if (binding == null) {
                return null;
            } else {
                String key = buildQueryKey(binding.getScName(), tbName);
                String sdValues = binding.getSdValues();
                if (sdValues != null) {
                    List<ShardRouteInfo> list = routeInfosCacheForSchemaLevel.get(key);
                    if (list == null) {
                        final List<ShardRouteInfo> routeInfos = new ArrayList<>();
                        final String scName0 = scName;
                        final String tbName0 = tbName;
                        new RangeExpressionParser(sdValues).visit(new RangeExpressionItemVisitor() {

                            @Override
                            public void visit(Object val) {
                                ShardRouteInfo routeInfo = getRouteInfo(scName0, tbName0, val);
                                routeInfos.add(routeInfo);
                            }
                        });
                        routeInfosCacheForSchemaLevel.put(key, routeInfos);
                        list = routeInfos;
                    }
                    return list;
                } else {
                    return null;
                }
            }
        }
    }

    /**
     * 
     * @param scName can be null
     * @return
     */
    private SimpleShardRouteRuleBinding getRouteRuleBindingBySchemaLevel(String scName) {
        if (scName != null) {
            return schemaBindingMap.get(scName);
        } else {
            return defaultSchemaBinding;
        }
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

    private ShardRouteInfo getRouteInfo(SimpleShardRouteRuleBinding binding, String scName, String tbName,
                                        Object sdValue) throws ShardRouteException, ShardValueNotFoundException {
        ShardRouteRule rule = binding.getRule();
        if (rule == null) {// 未配置rule,参数sdKey 和 sdValue都无效
            ShardRouteInfo info = new ShardRouteInfo();
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
                if (binding.getSdKey() != null && !(obj instanceof ShardRouteInfo)) {
                    throw new IllegalShardValueException(
                                                         "when 'sdKey' is configed in route rule for table '"
                                                                 + scName
                                                                 + "."
                                                                 + tbName
                                                                 + "', "
                                                                 + ", the type of 'sdValue' can only be 'RouteInfo'. but current 'sdValue' is "
                                                                 + obj.getClass() + "(" + obj + ")");
                }
                if (obj instanceof ShardRouteInfo) {
                    return (ShardRouteInfo) obj;
                } else {
                    return getRouteInfoByRouteRule(rule, scName, tbName, obj);
                }
            } else {
                return getRouteInfoByRouteRule(rule, scName, tbName, sdValue);
            }
        }
    }

    private ShardRouteInfo getRouteInfoByRouteRule(ShardRouteRule rule, String scName, String tbName, Object sdValue) {
        try {
            ShardRouteInfo info = new ShardRouteInfo();
            // throws exception
            String sc = rule.parseScName(scName, sdValue);
            // throws exception
            String tb = rule.parseTbName(tbName, sdValue);

            info.setScName(sc);
            info.setTbName(tb);
            return info;
        } catch (Throwable e) {
            throw new ShardRouteException("'scName':" + scName + ",'tbName':" + tbName + ",'sdValue':" + sdValue
                                          + ",routeRule:" + rule, e);
        }
    }
}
