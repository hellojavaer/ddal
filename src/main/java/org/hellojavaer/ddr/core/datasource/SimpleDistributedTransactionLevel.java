/*
 * Copyright 2016-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain schemaLevel copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hellojavaer.ddr.core.datasource;

import org.hellojavaer.ddr.core.utils.StringUtils;

/**
 *
 * @author <schemaLevel href="mailto:hellojavaer@gmail.com">zoukaiming[邹凯明]</schemaLevel>,created on 19/11/2016.
 */
public class SimpleDistributedTransactionLevel implements DistributedTransactionLevel {

    private byte schemaLevel;
    private byte tableLevel;

    public SimpleDistributedTransactionLevel(String level) {
        level = StringUtils.trim(level);
        if (level == null || level.length() != 2 || level.charAt(0) < '0' || level.charAt(0) > '9'
            || level.charAt(1) < '0' || level.charAt(1) > '9') {
            throw new IllegalArgumentException("level['" + level + "'] must be a string with tow digit");
        } else {
            this.schemaLevel = (byte) (level.charAt(0) - '0');
            this.tableLevel = (byte) (level.charAt(1) - '0');
        }
    }

    @Override
    public boolean isLimitSameDataSource() {
        return false;
    }

    @Override
    public boolean isLimitSameRuleForSchema() {
        return (schemaLevel & 2) == 0;
    }

    @Override
    public boolean isLimitSameLogicalNameForSchema() {
        return (schemaLevel & 1) == 0;
    }

    @Override
    public boolean isLimitSameRuleForTable() {
        return (tableLevel & 2) == 0;
    }

    @Override
    public boolean isLimitSameLogicalNameForTable() {
        return (tableLevel & 1) == 0;
    }
}
