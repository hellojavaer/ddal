/*
 * #%L
 * ddal-jsqlparser
 * %%
 * Copyright (C) 2016 - 2018 the original author or authors.
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

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Database;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.update.Update;
import org.hellojavaer.ddal.ddr.datasource.exception.CrossPreparedStatementException;
import org.hellojavaer.ddal.ddr.shard.RangeShardValue;
import org.hellojavaer.ddal.ddr.shard.ShardRouteConfig;
import org.hellojavaer.ddal.ddr.shard.ShardRouteInfo;
import org.hellojavaer.ddal.ddr.shard.ShardRouter;
import org.hellojavaer.ddal.ddr.shard.rule.SpelShardRouteRule;
import org.hellojavaer.ddal.ddr.shard.simple.SimpleShardParser;
import org.hellojavaer.ddal.ddr.shard.simple.SimpleShardRouteRuleBinding;
import org.hellojavaer.ddal.ddr.shard.simple.SimpleShardRouter;
import org.hellojavaer.ddal.ddr.sqlparse.SQLParsedResult;
import org.hellojavaer.ddal.ddr.sqlparse.SQLParsedState;
import org.hellojavaer.ddal.ddr.sqlparse.exception.*;
import org.hellojavaer.ddal.ddr.utils.DDRJSONUtils;
import org.hellojavaer.ddal.ddr.utils.DDRStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringReader;
import java.util.*;

/**
 *
 * JSQLParserAdapter sql解析适配器
 *   1.支持解析insert,update,delete,select 4种类型的sql解析,支持子语句嵌套和union all语法;
 *   2.如果sql匹配到分表信息
 *     2.1 分表select会被自动添加别名;
 *     2.2 分表非select操作不会被自动添加别名,如果column有表名前缀,前缀会根据路由信息动态被替换;
 *     2.3 分表路由值的获取
 *         2.3.1 如果路由规则中配置了分片字段,则检查sql参数及jdbc参数是否命中路由
 *             2.3.1.1 如果分表字段是不含'not'修饰符的'=','between'或'in()'操作,则命中分表路由;
 *             2.3.1.2 between和in操作允许混合使用sql参数和jdbc参数(eg:'id between(1 and ?)' 或者 'id in(1,2,?)');
 *         2.3.2 如果路由规则中未配置分片字段或(2.3.1)未命中路由值则通过ShardRoute注解或ShardRouteContext设置的路由信息进行路由,
 *         若ShardRouteContext中也未获取到匹配的路由信息则抛异常;
 *     (注:在指定分表字段后支持ShardRouteContext方式是为了能够提供'扫表'功能)
 *   3.如果解析过程如果没有匹配到分表配置,sql语句中的关键字格式化后返回(关键字大写);
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 12/11/2016.
 */
public class JSQLParserAdapter extends JSQLBaseVisitor {

    private Logger             logger              = LoggerFactory.getLogger(this.getClass());

    private String             sql;
    private ShardRouter        shardRouter;
    private Statement          statement;

    // the schemas which used in current sql
    private Set<String>        schemas             = new HashSet<>();

    private boolean            enableLimitCheck    = false;

    private List<TableWrapper> toBeConvertedTables = new ArrayList<>();

    static {
        try {
            checkJSqlParserFeature();
            checkCompatibilityWithJSqlParser();
        } catch (Throwable e) {
            throw new SQLParserCompatibilityException("JSqlParser feature check failed", e);
        }
    }

    /**
     * To make ddal-jsqlparser work well, JSqlParser should include the feature of 'support getting jdbc parameter index'.
     * And this feature is provided on the version of {@link <a href="https://github.com/JSQLParser/JSqlParser/releases/tag/jsqlparser-0.9.7">0.9.7</a>}.
     * This method is designed to check the necessary feature.
     */
    public static void checkJSqlParserFeature() throws JSQLParserException {
        CCJSqlParserManager parserManager = new CCJSqlParserManager();
        String sql = "SELECT * FROM tab_1 WHERE tab_1.col_1 = ? AND col_2 IN (SELECT DISTINCT col_2 FROM tab_2 WHERE col_3 LIKE ? AND col_4 > ?) LIMIT ?, ?";
        Select select = (Select) parserManager.parse(new StringReader(sql));
        PlainSelect selectBody = (PlainSelect) select.getSelectBody();
        //
        AndExpression andExpression = (AndExpression) selectBody.getWhere();
        EqualsTo equalsTo = (EqualsTo) andExpression.getLeftExpression();
        JdbcParameter jdbcParameter = (JdbcParameter) equalsTo.getRightExpression();
        Integer index1 = jdbcParameter.getIndex();
        if (index1 != 1) {
            throw new IllegalStateException("Current version of JSQLParser doesn't support the feature of 'support "
                                            + "get jdbc parameter index'");
        }
        //
        InExpression inExpression = (InExpression) andExpression.getRightExpression();
        SubSelect subSelect = (SubSelect) inExpression.getRightItemsList();
        PlainSelect subSelectBody = (PlainSelect) subSelect.getSelectBody();
        AndExpression subAndExpression = (AndExpression) subSelectBody.getWhere();
        LikeExpression likeExpression = (LikeExpression) subAndExpression.getLeftExpression();
        if (((JdbcParameter) likeExpression.getRightExpression()).getIndex() != 2) {
            throw new IllegalStateException(
                                            "Current version of JSQLParser doesn't support the feature of 'support get jdbc parameter index'");
        }
        //
        GreaterThan greaterThan = (GreaterThan) subAndExpression.getRightExpression();
        if (((JdbcParameter) greaterThan.getRightExpression()).getIndex() != 3) {
            throw new IllegalStateException(
                                            "Current version of JSQLParser doesn't support the feature of 'support get jdbc parameter index'");
        }
        //
        Expression offset = selectBody.getLimit().getOffset();
        Expression rowCount = selectBody.getLimit().getRowCount();
        if (((JdbcParameter) offset).getIndex() != 4 || ((JdbcParameter) rowCount).getIndex() != 5) {
            throw new IllegalStateException(
                                            "Current version of JSQLParser doesn't support the feature of 'support get jdbc parameter index'");
        }
    }

    public static void checkCompatibilityWithJSqlParser() {
        List<SimpleShardRouteRuleBinding> bindings = new ArrayList<>();
        SpelShardRouteRule numRule = new SpelShardRouteRule("{scName}_{format('%02d', sdValue % 8)}",
                                                            "{tbName}_{format('%04d', sdValue % 128)}");
        SimpleShardRouteRuleBinding user = new SimpleShardRouteRuleBinding();
        user.setScName("db");
        user.setTbName("user");
        user.setSdKey("id");
        user.setSdValues("[1..128]");
        user.setRule(numRule);
        bindings.add(user);
        SimpleShardRouter shardRouter = new SimpleShardRouter(bindings);
        SimpleShardParser parser = new SimpleShardParser(new JSQLParser(), shardRouter);
        // insert
        SQLParsedResult parsedResult = parser.parse("insert into db.user(id,`desc`) values(506,'desc')", null);
        if (!parsedResult.getSql().equals("INSERT INTO db_02.user_0122 (id, `desc`) VALUES (506, 'desc')")) {
            throw new IllegalStateException("ddal-jsqlparser is not compatibility with current version of JSqlParser");
        }
        // delete
        parsedResult = parser.parse("delete from db.user where id = 506 and `desc` = 'desc'", null);
        if (!parsedResult.getSql().equals("DELETE FROM db_02.user_0122 WHERE id = 506 AND `desc` = 'desc'")) {
            throw new IllegalStateException("ddal-jsqlparser is not compatibility with current version of JSqlParser");
        }
        // update
        parsedResult = parser.parse("update db.user set `desc` = 'desc' where id = 506", null);
        if (!parsedResult.getSql().equals("UPDATE db_02.user_0122 SET `desc` = 'desc' WHERE id = 506")) {
            throw new IllegalStateException("ddal-jsqlparser is not compatibility with current version of JSqlParser");
        }
        // select
        parsedResult = parser.parse("select * from db.user where id = 506 and `desc` = 'desc'", null);
        if (!parsedResult.getSql().equals("SELECT * FROM db_02.user_0122 AS user WHERE id = 506 AND `desc` = 'desc'")) {
            throw new IllegalStateException("ddal-jsqlparser is not compatibility with current version of JSqlParser");
        }
    }

    public JSQLParserAdapter(String sql, ShardRouter shardRouter, boolean enableLimitCheck) {
        this.sql = sql;
        this.shardRouter = shardRouter;
        this.enableLimitCheck = enableLimitCheck;
        try {
            this.statement = CCJSqlParserUtil.parse(sql);
        } catch (Throwable e) {
            throw new SQLSyntaxErrorException("sql is [" + sql + "]", e);
        }
        if (statement instanceof Select //
            || statement instanceof Update//
            || statement instanceof Insert//
            || statement instanceof Delete) {
            // ok
        } else {
            throw new UnsupportedSQLExpressionException(
                                                        "Sql ["
                                                                + sql
                                                                + "] is not supported in shard sql. Only support 'select' 'insert' 'update' and 'delete' sql statement");
        }
    }

    private String generateSplitString(String str) {
        Random random = new Random(System.currentTimeMillis());
        while (true) {
            long l = random.nextLong();
            if (l < 0) {
                l = -l;
            }
            String tar = "__" + l;
            if (str.indexOf(tar) < 0) {
                return tar;
            }
        }
    }

    public SQLParsedState parse() {
        try {
            statement.accept(this);
            String targetSql = statement.toString();
            //
            String splitString = generateSplitString(targetSql);
            for (int i = 0; i < toBeConvertedTables.size(); i++) {
                TableWrapper tab = toBeConvertedTables.get(i);
                // sql param的解析结果已经存储在routedFullTableName中,因此可以覆盖schemaName和name中的值;
                tab.setSchemaName(null);
                tab.setName("_" + i + splitString);
            }
            //
            targetSql = statement.toString();
            //
            final List<Object> splitSqls = new ArrayList<>();
            String[] sqls = targetSql.split(splitString);// table切分
            for (int i = 0; i < sqls.length - 1; i++) {
                String s = sqls[i];
                int index = s.lastIndexOf('_');
                splitSqls.add(s.substring(0, index));
                Integer paramIndex = Integer.valueOf(s.substring(index + 1));
                splitSqls.add(toBeConvertedTables.get(paramIndex));
            }
            splitSqls.add(sqls[sqls.length - 1]);
            //
            SQLParsedState parsedResult = new SQLParsedState() {

                @Override
                public SQLParsedResult parse(final Map<Object, Object> jdbcParams) {

                    final Map<TableWrapper, String> convertedTables = new HashMap<>();
                    final Set<String> schemas = new HashSet<>(JSQLParserAdapter.this.schemas);
                    final SQLParsedResult result = new SQLParsedResult() {

                        @Override
                        public void checkIfCrossPreparedStatement(final Map<Object, Object> jdbcParams)
                                                                                                       throws CrossPreparedStatementException {
                            for (Map.Entry<TableWrapper, String> entry : convertedTables.entrySet()) {
                                TableWrapper tab = entry.getKey();
                                route1(tab, jdbcParams, entry.getValue(), this.getSql());
                            }
                        }
                    };

                    StringBuilder sb = new StringBuilder();
                    for (Object obj : splitSqls) {
                        if (obj instanceof TableWrapper) {
                            TableWrapper tab = (TableWrapper) obj;
                            ShardRouteInfo routeInfo = route1(tab, jdbcParams, tab.getRoutedFullTableName(), null);
                            schemas.add(routeInfo.getScName());
                            String routedFullTableName = routeInfo.toString();
                            convertedTables.put(tab, routedFullTableName);
                            sb.append(routedFullTableName);
                        } else {
                            sb.append(obj);
                        }
                    }
                    result.setSql(sb.toString());
                    result.setSchemas(schemas);
                    return result;
                }
            };
            return parsedResult;
        } finally {
            // reset context
            this.getStack().clear();
        }
    }

    private ShardRouteInfo route1(TableWrapper tab, Map<Object, Object> jdbcParams, String routedFullTableName,
                                  String routedSql) {
        ShardRouteInfo routeInfo = null;
        // 1. no shard key
        if (tab.getJdbcParamKeys() == null || tab.getJdbcParamKeys().isEmpty()) {
            routeInfo = getRouteInfo(tab, null);
            if (routeInfo != null) {
                String fullTableName = routeInfo.toString();
                if (tab.getRoutedFullTableName() != null) {// 多重路由
                    if (!tab.getRoutedFullTableName().equals(fullTableName)) {
                        throw new AmbiguousRouteResultException("In sql[" + sql + "], table:'"
                                                                + tab.getOriginalConfig().toString()
                                                                + "' has multiple routing results["
                                                                + tab.getRoutedFullTableName() + "," + fullTableName
                                                                + "]");
                    }
                }
            } else {
                throw new GetRouteInfoException("Can't get route information for table:'"
                                                + tab.getOriginalConfig().toString()
                                                + "' 'sdValue':null and 'routeConfig':"
                                                + tab.getRouteConfig().toString());
            }
            return routeInfo;
        }
        // 2. jdbc param
        for (Object sqlParam : tab.getJdbcParamKeys()) {// size > 0
            if (sqlParam instanceof SqlParam) {
                Object sdValue = null;
                Object key = ((SqlParam) sqlParam).getValue();
                if (jdbcParams != null) {
                    sdValue = jdbcParams.get(key);
                }
                if (sdValue == null) {// sql中指定的sdValue不能为空
                    throw new IllegalSQLParameterException("For jdbc parameter key " + key
                                                           + ", jdbc parameter value is null. Jdbc parameter map is "
                                                           + DDRJSONUtils.toJSONString(jdbcParams) + " and sql is ["
                                                           + sql + "]");
                }
                routeInfo = getRouteInfo(tab, sdValue);
                String newRoutedFulltableName = routeInfo.toString();
                if (routedFullTableName == null) {
                    routedFullTableName = newRoutedFulltableName;
                } else {
                    verifyRoutedFullTableName(tab, jdbcParams, routedFullTableName, routedSql, newRoutedFulltableName);
                }
            } else {// range
                RangeParam rangeParam = (RangeParam) sqlParam;
                SqlParam begin = rangeParam.getBeginValue();
                SqlParam end = rangeParam.getEndValue();
                long s0 = 0;
                long e0 = 0;
                if (begin.isJdbcParamType()) {
                    Number number = (Number) jdbcParams.get(begin.getValue());
                    if (number == null) {
                        throw new IllegalSQLParameterException("Jdbc parameter can't be null. Jdbc parameter key is "
                                                               + begin.getValue() + ", jdbc parameter is "
                                                               + DDRJSONUtils.toJSONString(jdbcParams)
                                                               + " and sql is [" + sql + "]");
                    }
                    s0 = number.longValue();
                } else {
                    s0 = ((Number) begin.getValue()).longValue();
                }
                if (end.isJdbcParamType()) {
                    Number number = (Number) jdbcParams.get(end.getValue());
                    if (number == null) {
                        throw new IllegalSQLParameterException("Jdbc parameter can't be null. Jdbc parameter key is "
                                                               + end.getValue() + ", jdbc parameter is "
                                                               + DDRJSONUtils.toJSONString(jdbcParams)
                                                               + " and sql is [" + sql + "]");
                    }
                    e0 = number.longValue();
                } else {
                    e0 = ((Number) end.getValue()).longValue();
                }

                routeInfo = getRouteInfo(tab, new RangeShardValue(s0, e0));
                String next = routeInfo.toString();
                if (routedFullTableName == null) {
                    routedFullTableName = next;
                } else {
                    verifyRoutedFullTableName(tab, jdbcParams, routedFullTableName, routedSql, next);
                }

            }
        }
        return routeInfo;
    }

    private void verifyRoutedFullTableName(TableWrapper tab, Map<Object, Object> jdbcParams,
                                           String routedFullTableName, String routedSql, String newRoutedFulltableName) {
        if (!routedFullTableName.equals(newRoutedFulltableName)) {
            if (routedSql != null) {
                throw new CrossPreparedStatementException("Sql[" + sql + "] has been routed to [" + routedSql
                                                          + "] and table:'" + tab.getOriginalConfig().toString()
                                                          + "' has been route to '" + routedFullTableName
                                                          + "'. But current jdbc parameter:"
                                                          + DDRJSONUtils.toJSONString(jdbcParams)
                                                          + " require route to " + newRoutedFulltableName
                                                          + DDRJSONUtils.toJSONString(jdbcParams));
            } else {
                throw new AmbiguousRouteResultException("In sql[" + sql + "], table:'"
                                                        + tab.getOriginalConfig().toString()
                                                        + "' has multiple routing results[" + routedFullTableName + ","
                                                        + newRoutedFulltableName + "]. Jdbc parameter is "
                                                        + DDRJSONUtils.toJSONString(jdbcParams));
            }
        }
    }

    private void afterVisitBaseStatement() {
        FrameContext context = this.getStack().pop();
        for (Map.Entry<String, TableWrapper> entry : context.entrySet()) {
            TableWrapper tab = entry.getValue();
            if (tab == AMBIGUOUS_TABLE) {
                continue;
            }
            if (tab.getJdbcParamKeys() != null && !tab.getJdbcParamKeys().isEmpty()) {// 含jdbc路由
                toBeConvertedTables.add(tab);
            } else {// 不含jdbc路由
                if (tab.getRoutedFullTableName() == null) {// sql未路由
                    toBeConvertedTables.add(tab);
                }// else ok
            }
        }
    }

    private ShardRouteInfo getRouteInfo(TableWrapper tab, Object sdValue) {
        try {
            ShardRouteInfo routeInfo = this.shardRouter.getRouteInfo(tab.getOriginalConfig().getSchemaName(),
                                                                     tab.getOriginalConfig().getName(), sdValue);
            return routeInfo;
        } catch (Throwable e) {
            String fullTableName = null;
            if (tab.getOriginalConfig().getAlias() != null) {
                fullTableName = tab.getOriginalConfig().getAlias().getName();
            } else {
                fullTableName = tab.getOriginalConfig().getName();
            }
            String sdKey = null;
            if (tab.getRouteConfig().getSdKey() != null) {
                sdKey = fullTableName + "." + tab.getRouteConfig().getSdKey();
            }
            String msg = String.format("Current state is table:'%s', sdKey:'%s', sdValue:%s, routeConfig:%s, sql:[%s]",
                                       tab.getOriginalConfig().toString(), sdKey, sdValue,
                                       tab.getRouteConfig().toString(), sql);
            throw new GetRouteInfoException(msg, e);
        }
    }

    @Override
    public void visit(Insert insert) {
        this.getStack().push(new FrameContext(StatementType.INSERT));
        visit0(insert);
        // route table
        List<Column> columns = insert.getColumns();
        if (columns != null) {
            ItemsList itemsList = insert.getItemsList();
            if (itemsList instanceof ExpressionList) {
                procInsertColumns(columns, (ExpressionList) itemsList);
            } else if (itemsList instanceof MultiExpressionList) {
                for (ExpressionList expressionList : ((MultiExpressionList) itemsList).getExprList()) {
                    procInsertColumns(columns, expressionList);
                }
            } else {
                throw new UnsupportedSQLExpressionException(insert.toString());
            }
        }
        afterVisitBaseStatement();
    }

    private void procInsertColumns(List<Column> columns, ExpressionList expressionList) {
        List<Expression> valueList = expressionList.getExpressions();
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            TableWrapper tab = getTableFromContext(column);
            if (tab != null) {
                Expression expression = valueList.get(i);
                routeTable(tab, column, expression);
            }
        }
    }

    /**
     * mysql 'delete' doesn't support alais
     */
    @Override
    public void visit(Delete delete) {
        if (enableLimitCheck && delete.getLimit() == null) {
            throw new IllegalStateException("no limit in sql: " + sql);
        }
        this.getStack().push(new FrameContext(StatementType.DELETE));
        visit0(delete);
        afterVisitBaseStatement();
    }

    @Override
    public void visit(Update update) {
        if (enableLimitCheck && update.getLimit() == null) {
            throw new IllegalStateException("no limit in sql: " + sql);
        }
        this.getStack().push(new FrameContext(StatementType.UPDATE));
        visit0(update);
        afterVisitBaseStatement();
    }

    @Override
    public void visit(Select select) {
        if (enableLimitCheck && select.getSelectBody() != null && select.getSelectBody() instanceof PlainSelect
            && ((PlainSelect) select.getSelectBody()).getLimit() == null) {
            throw new IllegalStateException("no limit in sql: " + sql);
        }
        this.getStack().push(new FrameContext(StatementType.SELECT));
        visit0(select);
        afterVisitBaseStatement();
    }

    @Override
    public void visit(SubSelect subSelect) {
        this.getStack().push(new FrameContext(StatementType.SELECT));
        visit0(subSelect);
        afterVisitBaseStatement();

    }

    private TableWrapper getTableFromContext(Column col) {
        FrameContext frameContext = this.getStack().peek();
        String colFullName = col.toString();
        colFullName = DDRStringUtils.toLowerCase(colFullName);
        return frameContext.get(colFullName);
    }

    /**
     * value:'scName.tbAliasName' value:table
     * @param table
     * @param appendAlias
     */
    private void addRoutedTableIntoContext(TableWrapper table, ShardRouteConfig routeConfig, boolean appendAlias) {
        FrameContext frameContext = this.getStack().peek();
        String scName = table.getSchemaName();
        String tbName = table.getName();
        String tbAliasName = tbName;
        if (table.getAlias() != null && table.getAlias().getName() != null) {
            tbAliasName = table.getAlias().getName();
        } else {
            if (appendAlias) {
                table.setAlias(new Alias(tbName, true));
            }
        }
        String sdKey = DDRStringUtils.toLowerCase(routeConfig.getSdKey());// sdKey可以为null,当为null时需要通过context路由
        if (scName != null) {
            StringBuilder sb = new StringBuilder();
            sb.append(DDRStringUtils.toLowerCase(scName));
            sb.append('.');
            sb.append(DDRStringUtils.toLowerCase(tbAliasName));
            sb.append('.');
            sb.append(sdKey);
            String key = sb.toString();
            putIntoContext(frameContext, key, table);
            putIntoContext(frameContext, key.substring(scName.length() + 1), table);
            putIntoContext(frameContext, key.substring(scName.length() + 2 + tbAliasName.length()), table);
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append(DDRStringUtils.toLowerCase(tbAliasName));
            sb.append('.');
            sb.append(sdKey);
            String key = sb.toString();
            putIntoContext(frameContext, key, table);
            putIntoContext(frameContext, key.substring(tbAliasName.length() + 1), table);
        }
    }

    protected Object getRouteValue(Column column, Expression obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof LongValue) {
            return ((LongValue) obj).getValue();
        } else if (obj instanceof StringValue) {
            return ((StringValue) obj).getValue();
        } else if (obj instanceof HexValue) {
            return Long.parseLong(((HexValue) obj).getValue(), 16);
        } else if (obj instanceof DateValue) {
            return ((DateValue) obj).getValue();
        } else if (obj instanceof DoubleValue) {
            return ((DoubleValue) obj).getValue();
        } else if (obj instanceof TimeValue) {
            return ((TimeValue) obj).getValue();
        } else if (obj instanceof TimestampValue) {
            return ((TimestampValue) obj).getValue();
        } else {// NullValue
            throw new UnsupportedSQLParameterTypeException("Type '" + obj.getClass()
                                                           + "' is not supported for shard value '" + column.toString()
                                                           + "'. Sql is [" + sql + "]");
        }
    }

    @Override
    public void visit(InExpression inExpression) {
        if (inExpression.isNot()) {
            visit0(inExpression);
            return;
        }
        Column column = (Column) inExpression.getLeftExpression();
        if (inExpression.getRightItemsList() instanceof ExpressionList) {
            TableWrapper tab = getTableFromContext(column);
            if (tab == null) {
                visit0(inExpression);
                return;
            }
            ExpressionList itemsList = (ExpressionList) inExpression.getRightItemsList();
            List<Expression> list = itemsList.getExpressions();
            if (list == null || list.isEmpty()) {
                visit0(inExpression);
            }
            for (Expression exp : list) {
                routeTable(tab, column, exp);
            }
        } else {
            visit0(inExpression);
            return;
        }
    }

    @Override
    public void visit(Between between) {
        if (between.isNot()) {
            visit0(between);
            return;
        }
        Column column = (Column) between.getLeftExpression();
        TableWrapper tab = getTableFromContext(column);
        if (tab == null) {
            visit0(between);
            return;
        }
        Expression begin = between.getBetweenExpressionStart();
        Expression end = between.getBetweenExpressionEnd();
        if (begin instanceof SubSelect || end instanceof SubSelect) {
            visit0(between);
            return;
        } else if ((begin instanceof JdbcParameter || begin instanceof JdbcNamedParameter) //
                   || (end instanceof JdbcParameter || end instanceof JdbcNamedParameter)) {
            tab.getJdbcParamKeys().add(new RangeParam(new SqlParam(column, begin), new SqlParam(column, end)));
            return;
        } else {
            long s1 = ((Number) getRouteValue(column, begin)).longValue();
            long e1 = ((Number) getRouteValue(column, end)).longValue();
            if (s1 > e1) {
                long temp = s1;
                s1 = e1;
                e1 = temp;
            }
            routeTable(tab, column, new RangeShardValue(s1, e1));
        }
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        Column column = (Column) equalsTo.getLeftExpression();
        if (equalsTo.getRightExpression() instanceof SubSelect) {
            visit0(equalsTo);
            return;
        } else {
            String fullColumnName = column.toString();
            fullColumnName = DDRStringUtils.toLowerCase(fullColumnName);
            TableWrapper tab = this.getStack().peek().get(fullColumnName);
            if (tab != null) {// 需要路由的table
                routeTable(tab, column, equalsTo.getRightExpression());
            } else {// there maybe contains sub query,so we show invoke super.visit
                visit0(equalsTo);
            }
        }
    }

    private void routeTable(TableWrapper tab, Column column, Expression routeValueExpression) {
        // jdbc参数
        if (routeValueExpression != null && routeValueExpression instanceof JdbcParameter
            || routeValueExpression instanceof JdbcNamedParameter) {
            tab.getJdbcParamKeys().add(new SqlParam(column, routeValueExpression));
            return;
        } else {// 普通sql参数
            Object sdValue = getRouteValue(column, routeValueExpression);//
            routeTable(tab, column, sdValue);
        }
    }

    private void routeTable(TableWrapper tab, Column column, Object sdValue) {
        if (tab == null) {//
            return;
        }
        if (tab == AMBIGUOUS_TABLE) {
            throw new RuntimeException("Shard value '" + column.toString() + "' in where clause is ambiguous. Sql is ["
                                       + sql + "]");
        }
        ShardRouteInfo routeInfo = getRouteInfo(tab, sdValue);
        // 当没有设置别名,但分表字段使用了表前缀时,别前缀需要根据路由结果进行重写;
        // 这里之所有没有采用将table取名的方式是因为update/delete/insert不全支持别名;
        // 由于select都是支持别名的,所有都会被添加别名,因此不会执行下面的操作;
        Table columnTable = column.getTable();
        if (tab.getAlias() == null && columnTable != null && columnTable.getName() != null
            && columnTable.getName().length() > 0) {
            if (columnTable.getSchemaName() != null) {
                columnTable.setSchemaName(routeInfo.getScName());
            }
            columnTable.setName(routeInfo.getTbName());
        }
        route0(tab, routeInfo);
    }

    private void route0(TableWrapper tab, ShardRouteInfo routeInfo) {
        String fullTableName = routeInfo.toString();
        if (tab.getRoutedFullTableName() != null) {// 多重路由
            if (!tab.getRoutedFullTableName().equals(fullTableName)) {
                throw new AmbiguousRouteResultException("In sql[" + sql + "], table:'"
                                                        + tab.getOriginalConfig().toString()
                                                        + "' has multiple routing results["
                                                        + tab.getRoutedFullTableName() + "," + fullTableName + "]");
            }
        } else {// 是否使用alias在put的时候设置,这里只需要设置scName和tbName
            tab.setRoutedFullTableName(fullTableName);//
            tab.setSchemaName(routeInfo.getScName());
            tab.setName(routeInfo.getTbName());
            schemas.add(routeInfo.getScName());
        }
    }

    @Override
    public void visit(Table table) {
        String tbName = table.getName();

        ShardRouteConfig routeConfig = shardRouter.getRouteConfig(table.getSchemaName(), tbName);
        if (routeConfig != null) {
            TableWrapper tab = new TableWrapper(table, routeConfig);
            FrameContext frameContext = this.getStack().peek();
            StatementType statementType = frameContext.getStatementType();
            if (statementType == StatementType.SELECT) {
                addRoutedTableIntoContext(tab, routeConfig, true);
            } else {
                addRoutedTableIntoContext(tab, routeConfig, false);
            }
        }
    }

    protected void visit0(Select select) {
        super.visit(select);
    }

    protected void visit0(SubSelect subSelect) {
        super.visit(subSelect);
    }

    protected void visit0(Insert insert) {
        super.visit(insert);
    }

    protected void visit0(Update update) {
        super.visit(update);
    }

    protected void visit0(Delete delete) {
        super.visit(delete);
    }

    protected void visit0(EqualsTo equalsTo) {
        super.visit(equalsTo);
    }

    protected void visit0(InExpression inExpression) {
        super.visit(inExpression);
    }

    protected void visit0(Between between) {
        super.visit(between);
    }

    private void putIntoContext(FrameContext frameContext, String key, TableWrapper tab) {
        TableWrapper tab0 = frameContext.get(key);
        if (tab0 == null) {
            frameContext.put(key, tab);
        } else {
            frameContext.put(key, AMBIGUOUS_TABLE);
        }
    }

    private static final TableWrapper        AMBIGUOUS_TABLE = new TableWrapper(null, null);

    private ThreadLocal<Stack<FrameContext>> context         = new ThreadLocal<Stack<FrameContext>>() {

                                                                 @Override
                                                                 protected Stack<FrameContext> initialValue() {
                                                                     return new Stack<FrameContext>();
                                                                 }
                                                             };

    private enum StatementType {
        INSERT, DELETE, UPDATE, SELECT;
    }

    private class FrameContext extends HashMap<String, TableWrapper> {

        private StatementType statementType;

        public FrameContext(StatementType statementType) {
            this.statementType = statementType;
        }

        public StatementType getStatementType() {
            return statementType;
        }

    }

    private Stack<FrameContext> getStack() {
        return context.get();
    }

    private static class TableWrapper extends Table {

        public TableWrapper(Table table, ShardRouteConfig routeConfig) {
            this.routeConfig = routeConfig;
            if (table != null) {
                this.table = table;
                originalConfig.setDatabase(table.getDatabase());
                originalConfig.setSchemaName(table.getSchemaName());
                originalConfig.setName(table.getName());
                originalConfig.setAlias(table.getAlias());
                originalConfig.setPivot(table.getPivot());
                originalConfig.setASTNode(table.getASTNode());
            }
        }

        private Table            table;

        private Table            originalConfig = new Table();

        private ShardRouteConfig routeConfig;                       // route config info

        private String           routedFullTableName;               // 由routeInfo计算出,如果有sql路由时该字段不为空,如果该参数为空,表示需要jdbc路由

        private List<Object>     jdbcParamKeys  = new ArrayList<>(); // table 关联的jdbc列

        public ShardRouteConfig getRouteConfig() {
            return routeConfig;
        }

        public void setRouteConfig(ShardRouteConfig routeConfig) {
            this.routeConfig = routeConfig;
        }

        public String getRoutedFullTableName() {
            return routedFullTableName;
        }

        public void setRoutedFullTableName(String routedFullTableName) {
            this.routedFullTableName = routedFullTableName;
        }

        public List<Object> getJdbcParamKeys() {
            return jdbcParamKeys;
        }

        public void setJdbcParamKeys(List<Object> jdbcParamKeys) {
            this.jdbcParamKeys = jdbcParamKeys;
        }

        public Table getOriginalConfig() {
            return originalConfig;
        }

        public void setOriginalConfig(Table originalConfig) {
            this.originalConfig = originalConfig;
        }

        @Override
        public Database getDatabase() {
            return table.getDatabase();
        }

        @Override
        public void setDatabase(Database database) {
            table.setDatabase(database);
        }

        @Override
        public String getSchemaName() {
            return table.getSchemaName();
        }

        @Override
        public void setSchemaName(String string) {
            table.setSchemaName(string);
        }

        @Override
        public String getName() {
            return table.getName();
        }

        @Override
        public void setName(String string) {
            table.setName(string);
        }

        @Override
        public Alias getAlias() {
            return table.getAlias();
        }

        @Override
        public void setAlias(Alias alias) {
            table.setAlias(alias);
        }

        @Override
        public String getFullyQualifiedName() {
            return table.getFullyQualifiedName();
        }

        @Override
        public void accept(FromItemVisitor fromItemVisitor) {
            table.accept(fromItemVisitor);
        }

        @Override
        public void accept(IntoTableVisitor intoTableVisitor) {
            table.accept(intoTableVisitor);
        }

        @Override
        public Pivot getPivot() {
            return table.getPivot();
        }

        @Override
        public void setPivot(Pivot pivot) {
            table.setPivot(pivot);
        }

    }

    private class RangeParam {

        private SqlParam beginValue;
        private SqlParam endValue;

        public RangeParam(SqlParam beginValue, SqlParam endValue) {
            this.beginValue = beginValue;
            this.endValue = endValue;
        }

        public SqlParam getBeginValue() {
            return beginValue;
        }

        public void setBeginValue(SqlParam beginValue) {
            this.beginValue = beginValue;
        }

        public SqlParam getEndValue() {
            return endValue;
        }

        public void setEndValue(SqlParam endValue) {
            this.endValue = endValue;
        }
    }

    private class SqlParam {

        private Column     column;
        private Expression expression;
        private Object     value;
        private boolean    jdbcParamType = false;

        public SqlParam(Column column, Expression expression) {
            this.column = column;
            this.expression = expression;
            if (expression instanceof JdbcParameter) {
                value = ((JdbcParameter) expression).getIndex();
                jdbcParamType = true;
            } else if (expression instanceof JdbcNamedParameter) {
                value = ((JdbcNamedParameter) expression).getName();
                jdbcParamType = true;
            } else {
                value = getRouteValue(column, expression);
                jdbcParamType = false;
            }
        }

        public Column getColumn() {
            return column;
        }

        public void setColumn(Column column) {
            this.column = column;
        }

        public Expression getExpression() {
            return expression;
        }

        public void setExpression(Expression expression) {
            this.expression = expression;
        }

        public Object getValue() {
            return value;
        }

        public void setValue(Object value) {
            this.value = value;
        }

        public boolean isJdbcParamType() {
            return jdbcParamType;
        }

        public void setJdbcParamType(boolean jdbcParamType) {
            this.jdbcParamType = jdbcParamType;
        }
    }
}
