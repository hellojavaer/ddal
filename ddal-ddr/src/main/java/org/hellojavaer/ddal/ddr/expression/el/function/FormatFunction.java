/*
 * Copyright 2017-2018 the original author or authors.
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

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.Weighers;

import java.text.SimpleDateFormat;
import java.util.Map;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 25/04/2017.
 */
public class FormatFunction {

    private static ThreadLocal<Map> dateFormatCache = new ThreadLocal<Map>() {

                                                        @Override
                                                        protected Map initialValue() {
                                                            return new ConcurrentLinkedHashMap.Builder<String, Map>()//
                                                            .maximumWeightedCapacity(100)//
                                                            .weigher(Weighers.singleton())//
                                                            .build();
                                                        }
                                                    };

    public static String format(String format, Object... args) {
        return String.format(format, args);
    }

    public static String dateFormat(String format, Object date) {
        Map<String, SimpleDateFormat> map = dateFormatCache.get();
        SimpleDateFormat df = map.get(format);
        if (df == null) {
            df = new SimpleDateFormat(format);
            map.put(format, df);
        }
        return df.format(date);
    }

}
