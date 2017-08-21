/*
 * #%L
 * ddal-jsqlparser
 * %%
 * Copyright (C) 2016 - 2017 the original author or authors.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, either version 2.1 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Lesser Public License for more details.
 *
 * You should have received a copy of the GNU General Lesser Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/lgpl-2.1.html>.
 * #L%
 */
package org.hellojavaer.ddal.jsqlparser;

import org.hellojavaer.ddal.ddr.shard.ShardRouter;
import org.hellojavaer.ddal.ddr.sqlparse.SQLParsedState;
import org.hellojavaer.ddal.ddr.sqlparse.SQLParser;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 12/11/2016.
 */
public class JSQLParser implements SQLParser {

    private boolean enableLimitCheck = false;

    public boolean isEnableLimitCheck() {
        return enableLimitCheck;
    }

    public void setEnableLimitCheck(boolean enableLimitCheck) {
        this.enableLimitCheck = enableLimitCheck;
    }

    @Override
    public SQLParsedState parse(String sql, ShardRouter shardRouter) {
        JSQLParserAdapter sqlParser = new JSQLParserAdapter(sql, shardRouter, enableLimitCheck);
        return sqlParser.parse();
    }

}
