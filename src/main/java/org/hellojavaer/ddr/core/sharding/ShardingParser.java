package org.hellojavaer.ddr.core.sharding;

import java.util.Map;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">zoukaiming[邹凯明]</a>,created on 23/11/2016.
 */
public interface ShardingParser {

    String parse(String sql, Map<Integer, Object> jdbcParams);

}
