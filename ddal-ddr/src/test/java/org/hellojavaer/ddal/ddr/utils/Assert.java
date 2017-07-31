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
package org.hellojavaer.ddal.ddr.utils;

import java.util.List;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 16/12/2016.
 */
public class Assert {

    public static void equals(List src, List tar) {
        if (src == null && tar == null) {
            return;
        }
        if (src == null && tar != null || src != null && tar == null) {
            throw new IllegalArgumentException("[Assertion failed] - src or tar is null but the other is not null");
        }
        if (src.size() != tar.size()) {
            throw new IllegalArgumentException("[Assertion failed] - src.size()[" + src.size()
                                               + "] isn't equal to tar.size()[" + tar.size() + "]");
        }
        for (int i = 0; i < src.size(); i++) {
            Object obj0 = src.get(i);
            Object obj1 = tar.get(i);
            if (obj0 == null && obj1 == null) {
                return;
            } else {
                if (obj0 != null && !obj0.equals(obj1) && obj1 != null && !obj1.equals(obj0)) {
                    throw new IllegalArgumentException("[Assertion failed] - src is not equate to tar at index:" + i);
                }
            }
        }
    }

    public static void isTrue(boolean expression) {
        isTrue(expression, "[Assertion failed] - this expression must be true");
    }

    public static void isTrue(boolean expression, String message) {
        if (!expression) {
            throw new IllegalArgumentException(message);
        }
    }

    public static void equals(String str0, String str1) {
        if (str0 == str1) {
            return;
        }
        if (str0 == null && str1 != null //
            || str0 != null && str1 == null//
            || !str0.equals(str1)//
        ) {
            throw new IllegalArgumentException("[Assertion failed] - str0 is not equal to str1. str0 is '" + str0
                                               + "' and str1 is '" + str1 + "'");
        }
    }
}
