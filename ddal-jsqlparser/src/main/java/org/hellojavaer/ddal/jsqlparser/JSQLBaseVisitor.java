/*
 * #%L
 * JSQLParser library
 * %%
 * Copyright (C) 2016 - 2017 JSQLParser
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

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.util.TablesNamesFinder;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 12/11/2016.
 */
public class JSQLBaseVisitor extends TablesNamesFinder {

    public void init() {
        // do nothing
    }

    public void init(boolean allowColumnProcessing) {
        // do nothing
    }

    @Override
    public void visit(Table tableName) {
        // do nothing
    }

    @Override
    public void visit(WithItem withItem) {
        withItem.getSelectBody().accept(this);
    }

    @Override
    public void visit(Column tableColumn) {
        // do nothing
    }

}
