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
package org.hellojavaer.ddal.datasource.spring;

import org.springframework.beans.BeanInfoFactory;

import java.awt.*;
import java.beans.*;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 12/09/2017.
 */
public class DDALBeanInfoFactory implements BeanInfoFactory {

    private static final String                  DDAL_PACKAGE = "org.hellojavaer.ddal.";
    private static final Map<Class<?>, BeanInfo> cache        = new HashMap<>();

    @Override
    public BeanInfo getBeanInfo(Class<?> beanClass) throws IntrospectionException {
        if (beanClass == null) {
            return null;
        }
        Introspector.getBeanInfo(beanClass);
        if (beanClass.getName().startsWith(DDAL_PACKAGE)) {
            BeanInfo beanInfo = cache.get(beanClass);
            if (beanInfo == null) {
                beanInfo = new DDALBeanInfo(beanClass, Introspector.getBeanInfo(beanClass));
                cache.put(beanClass, beanInfo);
            }
            return beanInfo;
        } else {
            return null;
        }
    }

    class DDALBeanInfo implements BeanInfo {

        private BeanInfo beanInfo;

        public DDALBeanInfo(Class<?> beanClass, BeanInfo beanInfo) {
            this.beanInfo = beanInfo;
            PropertyDescriptor[] pds = beanInfo.getPropertyDescriptors();
            if (pds != null) {
                for (PropertyDescriptor pd : pds) {
                    if (pd != null) {
                        if (pd.getWriteMethod() == null || pd.getReadMethod() == null) {
                            Class<?> clazz = beanClass;
                            do {
                                Class<?> fieldType = pd.getPropertyType();
                                if (pd.getReadMethod() == null) {
                                    try {
                                        Method method = clazz.getDeclaredMethod(fieldToGetter(pd.getName(), fieldType));
                                        method.setAccessible(true);
                                        pd.setReadMethod(method);
                                        pd.getPropertyType();
                                    } catch (Exception ignored) {
                                    }
                                }
                                if (pd.getWriteMethod() == null) {
                                    try {
                                        Method method = clazz.getDeclaredMethod(fieldToSetter(pd.getName()), fieldType);
                                        method.setAccessible(true);
                                        pd.setWriteMethod(method);
                                    } catch (Exception ignored) {
                                    }
                                }
                                if (pd.getReadMethod() != null && pd.getWriteMethod() != null) {
                                    break;
                                }
                                clazz = clazz.getSuperclass();
                            } while (clazz != null && clazz != Object.class);
                        }
                    }
                }
            }
        }

        private String fieldToGetter(String name, Class<?> type) {
            return fieldToGetter(name, type == Boolean.class || type == boolean.class);
        }

        private String fieldToGetter(String name, boolean isBoolean) {
            return (isBoolean ? "is" : "get") + name.substring(0, 1).toUpperCase() + name.substring(1);
        }

        private String fieldToSetter(String name) {
            return "set" + name.substring(0, 1).toUpperCase() + name.substring(1);
        }

        @Override
        public BeanDescriptor getBeanDescriptor() {
            return beanInfo.getBeanDescriptor();
        }

        @Override
        public EventSetDescriptor[] getEventSetDescriptors() {
            return beanInfo.getEventSetDescriptors();
        }

        @Override
        public int getDefaultEventIndex() {
            return beanInfo.getDefaultEventIndex();
        }

        @Override
        public PropertyDescriptor[] getPropertyDescriptors() {
            return beanInfo.getPropertyDescriptors();
        }

        @Override
        public int getDefaultPropertyIndex() {
            return beanInfo.getDefaultPropertyIndex();
        }

        @Override
        public MethodDescriptor[] getMethodDescriptors() {
            return beanInfo.getMethodDescriptors();
        }

        @Override
        public BeanInfo[] getAdditionalBeanInfo() {
            return beanInfo.getAdditionalBeanInfo();
        }

        @Override
        public Image getIcon(int iconKind) {
            return beanInfo.getIcon(iconKind);
        }
    }
}
