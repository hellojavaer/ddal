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
package org.hellojavaer.ddr.core.expression.range;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 30/11/2016.
 */
public class RangeExpressionException extends RuntimeException {

    public RangeExpressionException(String string, int index, char currentChar, String expectedChars) {
        super("Unexpected character '" + (currentChar == 0 ? "null" : currentChar) + "' at index " + index
              + ". Source string is " + string + ".Detail message is :" + expectedChars);
    }

    public RangeExpressionException(String string, int index, char currentChar, char... expectedChars) {
        super(build(string, index, currentChar, expectedChars));
    }

    private static String build(String string, int index, char currentChar, char... expectedChars) {
        StringBuilder sb = new StringBuilder();
        sb.append("Unexpected character '");
        if (currentChar == 0) {
            sb.append("null");
        } else {
            sb.append(currentChar);
        }
        sb.append("' at index ");
        sb.append(index);
        sb.append(", expect ");
        if (expectedChars == null) {
            sb.append("null");
        } else {
            sb.append("'");
            int i = 0;
            for (; i < expectedChars.length - 1; i++) {
                sb.append(expectedChars[i]);
                sb.append(',');
            }
            sb.append(expectedChars[i]);
            sb.append("'");
        }
        sb.append(". Source string is ");
        sb.append(string);
        return sb.toString();
    }
}
