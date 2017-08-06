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
package org.hellojavaer.ddal.ddr.expression.range;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 30/11/2016.
 */
public class RangeExpressionException extends RuntimeException {

    public RangeExpressionException(String expression, int pos, String msg) {
        super(build(expression, pos, msg));
    }

    private static String build(String expression, int position, String msg) {
        StringBuilder output = new StringBuilder();
        output.append("Expression '");
        output.append(expression);
        output.append("'");
        if (position != -1) {
            output.append(" @ ");
            output.append(position);
        }
        output.append(": ");
        output.append(msg);
        return output.toString();
    }
}
