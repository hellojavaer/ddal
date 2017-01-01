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
package org.hellojavaer.ddr.expression.format.ast.sytax;

import org.hellojavaer.ddr.expression.format.FormatExpressionContext;

import java.util.List;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 17/11/2016.
 */
public class FeCompoundExpression extends FeNodeImpl {

    public FeCompoundExpression() {
    }

    public void setChildren(List<FeNodeImpl> children) {
        this.children = children;
    }

    @Override
    public String getValue(FormatExpressionContext context) {
        StringBuilder sb = new StringBuilder();
        if (super.children != null) {
            for (FeNode node : children) {
                String val = node.getValue(context);
                if (val != null) {
                    sb.append(val);
                } else {
                    sb.append("null");
                }
            }
            return sb.toString();
        } else {
            return null;
        }
    }

}
