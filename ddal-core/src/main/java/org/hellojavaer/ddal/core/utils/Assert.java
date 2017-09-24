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
package org.hellojavaer.ddal.core.utils;

import java.util.*;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 16/12/2016.
 */
public class Assert {

    public static void equals(Map<? extends Object, ? extends Object> src, Map<? extends Object, ? extends Object> tar) {
        if (src == null && tar == null) {
            return;
        } else if (src == null && tar != null) {
            throw new IllegalArgumentException("[Assertion failed] - src is null, but tar is '" + tar + "'");
        } else if (src != null && tar == null) {
            throw new IllegalArgumentException("[Assertion failed] - src is '" + src + "', but tar is null");
        }
        if (src.size() != tar.size()) {
            throw new IllegalArgumentException("[Assertion failed] - src.size() '" + src.size()
                                               + "' isn't equal to tar.size() '" + tar.size() + "'");
        }
        Iterator<? extends Map.Entry<?, ?>> srcIterator = src.entrySet().iterator();
        Iterator<? extends Map.Entry<?, ?>> tarIterator = tar.entrySet().iterator();
        int count = -1;
        while (srcIterator.hasNext() && tarIterator.hasNext()) {
            count++;
            Map.Entry<?, ?> srcEntry = srcIterator.next();
            Map.Entry<?, ?> tarEntry = tarIterator.next();
            try {
                equals(srcEntry.getKey(), tarEntry.getKey());
                equals(srcEntry.getValue(), tarEntry.getValue());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("[Assertion failed] - src or tar is not equal to tar at index "
                                                   + count, e);
            }
        }
    }

    public static void equals(Collection<? extends Object> src, Collection<? extends Object> tar) {
        if (src == null && tar == null) {
            return;
        } else if (src == null && tar != null) {
            throw new IllegalArgumentException("[Assertion failed] - src is null, but tar is '" + tar + "'");
        } else if (src != null && tar == null) {
            throw new IllegalArgumentException("[Assertion failed] - src is '" + src + "', but tar is null");
        }
        if (src.size() != tar.size()) {
            throw new IllegalArgumentException("[Assertion failed] - src.size() '" + src.size()
                                               + "' isn't equal to tar.size() '" + tar.size() + "'");
        }
        Iterator<?> srcIterator = src.iterator();
        Iterator<?> tarIterator = tar.iterator();
        int count = -1;
        while (srcIterator.hasNext() && tarIterator.hasNext()) {
            count++;
            Object obj0 = srcIterator.next();
            Object obj1 = tarIterator.next();
            try {
                equals(obj0, obj1);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("[Assertion failed] - src or tar is not equal to tar at index "
                                                   + count, e);
            }
        }
    }

    public static void equals(Object src, Object tar) {
        if (src == null && tar == null) {
            return;
        } else if (src == null && tar != null) {
            throw new IllegalArgumentException("[Assertion failed] - src is null, but tar is '" + tar + "'");
        } else if (src != null && tar == null) {
            throw new IllegalArgumentException("[Assertion failed] - src is '" + src + "', but tar is null");
        }
        if (src instanceof Map && tar instanceof Map) {
            equals((Map) src, (Map) tar);
        } else if (src instanceof Collection && tar instanceof Collection) {
            equals((Collection) src, (Collection) tar);
        } else {
            if (!src.equals(tar)) {
                throw new IllegalArgumentException("[Assertion failed] - src '" + src + "' is not equal to tar '" + tar
                                                   + "'");
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

    public static void notNull(Object obj, String msg) {
        isTrue(obj != null, msg);
    }

    public static void notNull(Object obj) {
        isTrue(obj != null, "[Assertion failed] - this object can't be null");
    }
}
