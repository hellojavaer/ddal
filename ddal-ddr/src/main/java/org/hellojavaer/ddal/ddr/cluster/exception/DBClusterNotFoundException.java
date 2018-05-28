/*
 * Copyright 2018-2018 the original author or authors.
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
package org.hellojavaer.ddal.ddr.cluster.exception;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 2018/5/27.
 */
public class DBClusterNotFoundException extends DBClusterException {

    public DBClusterNotFoundException() {
    }

    public DBClusterNotFoundException(String message) {
        super(message);
    }

    public DBClusterNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public DBClusterNotFoundException(Throwable cause) {
        super(cause);
    }

    public DBClusterNotFoundException(String message, Throwable cause, boolean enableSuppression,
                                      boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
