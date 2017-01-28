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
            if (!src.get(i).equals(tar.get(i))) {
                throw new IllegalArgumentException("[Assertion failed] - src is not equate to tar at index:" + i);
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
