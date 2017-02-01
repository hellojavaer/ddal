/*
 * Copyright 2017-2017 the original author or authors.
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
package org.hellojavaer.ddal.ddr.expression.el.function;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 10/01/2017.
 */
public class MathFunction {

    public static Number abs(Number number) {
        if (number == null) {
            return null;
        }
        if (number instanceof Byte) {
            byte b = number.byteValue();
            if (b < 0) {
                return (byte) -b;
            }
        } else if (number instanceof Short) {
            short s = number.shortValue();
            if (s < 0) {
                return (short) -s;
            }
        } else if (number instanceof Integer) {
            int i = number.intValue();
            if (i < 0) {
                return (int) -i;
            }
        } else if (number instanceof Long) {
            long l = number.longValue();
            if (l < 0) {
                return (long) -l;
            }
        } else if (number instanceof Float) {
            float f = number.floatValue();
            if (f < 0) {
                return (float) -f;
            }
        } else if (number instanceof Double) {
            double d = number.doubleValue();
            if (d < 0) {
                return (double) -d;
            }
        } else if (number instanceof BigDecimal) {
            BigDecimal bigDecimal = (BigDecimal) number;
            return bigDecimal.abs();
        } else if (number instanceof BigInteger) {
            BigInteger bigInteger = (BigInteger) number;
            bigInteger.abs();
        } else {
            throw new UnsupportedOperationException("Can't calculate absolute value for type of " + number.getClass());
        }
        return number;
    }

    public static Number max(Number n1, Number n2) {
        if (compare(n1, n2) >= 0) {
            return n1;
        } else {
            return n2;
        }
    }

    public static Number min(Number n1, Number n2) {
        if (compare(n1, n2) <= 0) {
            return n1;
        } else {
            return n2;
        }
    }

    private static int compare(Number n1, Number n2) {
        if (n1 instanceof BigInteger) {
            BigInteger b1 = (BigInteger) n1;
            BigInteger b2 = (BigInteger) n2;
            return b1.compareTo(b2);
        } else if (n1 instanceof BigDecimal) {
            BigDecimal d1 = (BigDecimal) n1;
            BigDecimal d2 = (BigDecimal) n2;
            return d1.compareTo(d2);
        } else if (n1 instanceof Double || n2 instanceof Double) {
            double d1 = n1.doubleValue();
            double d2 = n2.doubleValue();
            return (d1 - d2 > 0) ? 1 : -1;
        } else if (n1 instanceof Float || n2 instanceof Float) {
            float f1 = n1.floatValue();
            float f2 = n2.floatValue();
            return (f1 - f2 > 0) ? 1 : -1;
        } else {
            long l1 = n1.longValue();
            long l2 = n2.longValue();
            return (int) (l1 - l2);
        }
    }

    public static long ceil(double a) {
        return (long) java.lang.Math.ceil(a);
    }

    public static long floor(double a) {
        return (long) java.lang.Math.floor(a);
    }

    public static long round(double a) {
        return java.lang.Math.round(a);
    }

}
