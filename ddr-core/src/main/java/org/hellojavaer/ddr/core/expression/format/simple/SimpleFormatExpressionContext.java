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
package org.hellojavaer.ddr.core.expression.format.simple;

import org.hellojavaer.ddr.core.expression.format.FormatExpressionContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 15/11/2016.
 */
public class SimpleFormatExpressionContext implements FormatExpressionContext {

    private Map<String, Object> map = new HashMap<String, Object>();

    public void setVariable(String name, Object value) {
        map.put(name, value);
    }

    public Object getVariable(String name) {
        return map.get(name);
    }

    public Set<Map.Entry<String, Object>> entrySet() {
        return map.entrySet();
    }
}
