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
package org.hellojavaer.ddr.core.sharding;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hellojavaer.ddr.core.exception.DDRException;
import org.hellojavaer.ddr.core.sharding.config.SimpleShardingRouterRuleBinding;
import org.hellojavaer.ddr.core.sharding.config.SimpleShardingConfiguration;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">zoukaiming[邹凯明]</a>,created on 14/11/2016.
 */
public class SimpleShardingRouter implements ShardingRouter {

    private final Log                                    logger = LogFactory.getLog(getClass());
    private SimpleShardingConfiguration                  configuration;
    private Map<String, SimpleShardingRouterRuleBinding> cache  = Collections.EMPTY_MAP;

    public void setConfiguration(SimpleShardingConfiguration configuration) {
        this.configuration = configuration;
        refreshCache();
    }

    public SimpleShardingConfiguration getConfiguration() {
        return configuration;
    }

    private void refreshCache() {
        ConcurrentHashMap<String, SimpleShardingRouterRuleBinding> temp = new ConcurrentHashMap<String, SimpleShardingRouterRuleBinding>();
        List<SimpleShardingRouterRuleBinding> bindings = configuration.getBindings();
        if (bindings != null) {
            for (SimpleShardingRouterRuleBinding binding : bindings) {
                if (binding.getRule() == null) {
                    throw new DDRException("binding for scName:" + binding.getScName() + " tbName:"
                                           + binding.getTbName() + " sdName:" + binding.getSdName() + " can't be null");
                }
                String scName = filter(binding.getScName());
                String tbName = filter(binding.getTbName());
                String sdName = filter(binding.getSdName());
                if (tbName == null) {
                    throw new DDRException("tbName can't be empty");
                }
                if (scName != null) {
                    StringBuilder sb = new StringBuilder();
                    sb.append(scName).append('.').append(tbName);
                    putToCache(temp, sb.toString(), binding);
                }
                SimpleShardingRouterRuleBinding b0 = new SimpleShardingRouterRuleBinding();
                b0.setScName(scName);
                b0.setTbName(tbName);
                b0.setSdName(sdName);
                b0.setRule(binding.getRule());
                putToCache(temp, tbName, b0);
            }
        }
        this.cache = temp;
    }

    private void putToCache(ConcurrentHashMap<String, SimpleShardingRouterRuleBinding> cache, String key,
                            SimpleShardingRouterRuleBinding val) {
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
    public void beginExecution(ShardingRouterContext context) {

    }

    @Override
    public void endExecution(ShardingRouterContext context) {

    }

    @Override
    public void beginStatement(ShardingRouterContext context, int type) {
    }

    @Override
    public void endStatement(ShardingRouterContext context) {
    }

    @Override
    public void beginSubSelect(ShardingRouterContext context) {

    }

    @Override
    public void endSubSelect(ShardingRouterContext context) {

    }

    @Override
    public boolean isRoute(ShardingRouterContext context, String scName, String tbName) {
        scName = filter(scName);
        tbName = filter(tbName);
        if (getBinding(scName, tbName) == null) {
            return false;
        } else {
            return true;
        }
    }

    private SimpleShardingRouterRuleBinding getBinding(String scName, String tbName) {
        if (tbName == null) {
            throw new DDRException("tbName can't be empty");
        }
        if (scName == null) {
            return cache.get(tbName);
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append(scName).append('.').append(tbName);
            SimpleShardingRouterRuleBinding binding = cache.get(sb.toString());// 全名称匹配
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
    public String getRouteColName(ShardingRouterContext context, String scName, String tbName) {
        scName = filter(scName);
        tbName = filter(tbName);
        SimpleShardingRouterRuleBinding binding = getBinding(scName, tbName);
        if (binding == null) {
            throw new DDRException("");
        } else {
            return binding.getSdName();
        }
    }

    @Override
    public ShardingInfo route(ShardingRouterContext context, String scName, String tbName, Long sdValue) {
        scName = filter(scName);
        tbName = filter(tbName);
        SimpleShardingRouterRuleBinding binding = getBinding(scName, tbName);
        if (binding == null) {
            throw new DDRException("no binding for  scName:" + scName + " tbName:" + tbName);
        } else {
            if (scName == null) {
                scName = binding.getScName();
            }
            if (binding.getSdName() == null) {
                if (sdValue != null) {
                    logger.warn("scName:" + scName + " tbName:" + tbName + " sdName:null , sdValue expect null but "
                                + sdValue);
                }
                sdValue = ShardingContext.getRouteValue(scName, tbName);
                if (sdValue == null) {
                    throw new DDRException("you should invoke ShardingContext to set [scName:" + scName + ",tbName:"
                                           + tbName + "] sdName value");
                }
            } else {
                if (sdValue == null) {
                    throw new DDRException("sdValue can't be null for scName:" + scName + " and tbName:" + tbName);
                }
            }
            Object $0 = binding.getRule().parseScRoute(scName, sdValue);
            String sc = binding.getRule().parseScFormat(scName, $0);
            Object $0_ = binding.getRule().parseTbRoute(tbName, sdValue);
            String tb = binding.getRule().parseTbFormat(tbName, $0_);
            if (tb == null) {
                throw new DDRException("");
            }
            ShardingInfo info = new ShardingInfo();
            info.setScName(sc);
            info.setTbName(tb);
            return info;
        }
    }
};
