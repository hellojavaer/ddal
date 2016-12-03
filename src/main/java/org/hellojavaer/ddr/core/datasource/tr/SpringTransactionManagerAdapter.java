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
package org.hellojavaer.ddr.core.datasource.tr;

import org.hellojavaer.ddr.core.datasource.TransactionManager;
import org.hellojavaer.ddr.core.datasource.TransactionManagerAdapter;

import java.lang.reflect.Method;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">zoukaiming[邹凯明]</a>,created on 03/12/2016.
 */
public class SpringTransactionManagerAdapter implements TransactionManagerAdapter {

    @Override
    public void adapt() {
        try {
            Method m0 = getIsCurrentTransactionReadOnlyMethod();
            Boolean readOnly = (Boolean) m0.invoke(null, null);
            TransactionManager.setReadOnly(readOnly);

            Method m1 = getIsolationLevelMethod();
            Integer isolationLevel = (Integer) m1.invoke(null, null);
            TransactionManager.setIsolationLevel(isolationLevel);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Method isCurrentTransactionReadOnly = null;
    private static Method getIsolationLevel            = null;

    private static Method getIsCurrentTransactionReadOnlyMethod() {
        if (isCurrentTransactionReadOnly == null) {
            synchronized (SpringTransactionManagerAdapter.class) {
                if (isCurrentTransactionReadOnly == null) {
                    Method method = null;
                    try {
                        Class clazz = Class.forName("org.springframework.transaction.support.TransactionSynchronizationManager");
                        if (clazz == null) {
                            throw new ClassNotFoundException("org.springframework.transaction.support.TransactionSynchronizationManager");
                        } else {
                            method = clazz.getMethod("isCurrentTransactionReadOnly");
                        }
                        if (method == null) {
                            throw new NoSuchMethodException("org.springframework.transaction.support.TransactionSynchronizationManager#isCurrentTransactionReadOnly");
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    isCurrentTransactionReadOnly = method;
                }
            }
        }
        return isCurrentTransactionReadOnly;
    }

    private static Method getIsolationLevelMethod() {
        if (getIsolationLevel == null) {
            synchronized (SpringTransactionManagerAdapter.class) {
                if (getIsolationLevel == null) {
                    Method method = null;
                    try {
                        Class clazz = Class.forName("org.springframework.transaction.support.TransactionSynchronizationManager");
                        if (clazz == null) {
                            throw new ClassNotFoundException("org.springframework.transaction.support.TransactionSynchronizationManager");
                        } else {
                            method = clazz.getMethod("getIsolationLevel");
                        }
                        if (method == null) {
                            throw new NoSuchMethodException("org.springframework.transaction.support.TransactionSynchronizationManager#getIsolationLevel");
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    getIsolationLevel = method;
                }
            }
        }
        return getIsolationLevel;
    }

}
