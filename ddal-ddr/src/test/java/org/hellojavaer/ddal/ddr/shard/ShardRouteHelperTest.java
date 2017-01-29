package org.hellojavaer.ddal.ddr.shard;

import org.hellojavaer.ddal.ddr.utils.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 18/12/2016.
 */
public class ShardRouteHelperTest extends BaseShardParserTest {

    @Test
    public void test00() {
        List<String> expectedResult = new ArrayList();
        expectedResult.add("db_00.user_0000");
        expectedResult.add("db_01.user_0001");
        expectedResult.add("db_02.user_0002");
        expectedResult.add("db_03.user_0003");
        expectedResult.add("db_04.user_0004");
        expectedResult.add("db_05.user_0005");
        expectedResult.add("db_06.user_0006");
        expectedResult.add("db_07.user_0007");
        expectedResult.add("db_00.user_0008");
        expectedResult.add("db_01.user_0009");
        expectedResult.add("db_02.user_0010");
        expectedResult.add("db_03.user_0011");
        expectedResult.add("db_04.user_0012");
        expectedResult.add("db_05.user_0013");
        expectedResult.add("db_06.user_0014");
        expectedResult.add("db_07.user_0015");
        expectedResult.add("db_00.user_0016");
        expectedResult.add("db_01.user_0017");
        expectedResult.add("db_02.user_0018");
        expectedResult.add("db_03.user_0019");
        expectedResult.add("db_04.user_0020");
        expectedResult.add("db_05.user_0021");
        expectedResult.add("db_06.user_0022");
        expectedResult.add("db_07.user_0023");
        expectedResult.add("db_00.user_0024");
        expectedResult.add("db_01.user_0025");
        expectedResult.add("db_02.user_0026");
        expectedResult.add("db_03.user_0027");
        expectedResult.add("db_04.user_0028");
        expectedResult.add("db_05.user_0029");
        expectedResult.add("db_06.user_0030");
        expectedResult.add("db_07.user_0031");
        expectedResult.add("db_00.user_0032");
        expectedResult.add("db_01.user_0033");
        expectedResult.add("db_02.user_0034");
        expectedResult.add("db_03.user_0035");
        expectedResult.add("db_04.user_0036");
        expectedResult.add("db_05.user_0037");
        expectedResult.add("db_06.user_0038");
        expectedResult.add("db_07.user_0039");
        expectedResult.add("db_00.user_0040");
        expectedResult.add("db_01.user_0041");
        expectedResult.add("db_02.user_0042");
        expectedResult.add("db_03.user_0043");
        expectedResult.add("db_04.user_0044");
        expectedResult.add("db_05.user_0045");
        expectedResult.add("db_06.user_0046");
        expectedResult.add("db_07.user_0047");
        expectedResult.add("db_00.user_0048");
        expectedResult.add("db_01.user_0049");
        expectedResult.add("db_02.user_0050");
        expectedResult.add("db_03.user_0051");
        expectedResult.add("db_04.user_0052");
        expectedResult.add("db_05.user_0053");
        expectedResult.add("db_06.user_0054");
        expectedResult.add("db_07.user_0055");
        expectedResult.add("db_00.user_0056");
        expectedResult.add("db_01.user_0057");
        expectedResult.add("db_02.user_0058");
        expectedResult.add("db_03.user_0059");
        expectedResult.add("db_04.user_0060");
        expectedResult.add("db_05.user_0061");
        expectedResult.add("db_06.user_0062");
        expectedResult.add("db_07.user_0063");
        expectedResult.add("db_00.user_0064");
        expectedResult.add("db_01.user_0065");
        expectedResult.add("db_02.user_0066");
        expectedResult.add("db_03.user_0067");
        expectedResult.add("db_04.user_0068");
        expectedResult.add("db_05.user_0069");
        expectedResult.add("db_06.user_0070");
        expectedResult.add("db_07.user_0071");
        expectedResult.add("db_00.user_0072");
        expectedResult.add("db_01.user_0073");
        expectedResult.add("db_02.user_0074");
        expectedResult.add("db_03.user_0075");
        expectedResult.add("db_04.user_0076");
        expectedResult.add("db_05.user_0077");
        expectedResult.add("db_06.user_0078");
        expectedResult.add("db_07.user_0079");
        expectedResult.add("db_00.user_0080");
        expectedResult.add("db_01.user_0081");
        expectedResult.add("db_02.user_0082");
        expectedResult.add("db_03.user_0083");
        expectedResult.add("db_04.user_0084");
        expectedResult.add("db_05.user_0085");
        expectedResult.add("db_06.user_0086");
        expectedResult.add("db_07.user_0087");
        expectedResult.add("db_00.user_0088");
        expectedResult.add("db_01.user_0089");
        expectedResult.add("db_02.user_0090");
        expectedResult.add("db_03.user_0091");
        expectedResult.add("db_04.user_0092");
        expectedResult.add("db_05.user_0093");
        expectedResult.add("db_06.user_0094");
        expectedResult.add("db_07.user_0095");
        expectedResult.add("db_00.user_0096");
        expectedResult.add("db_01.user_0097");
        expectedResult.add("db_02.user_0098");
        expectedResult.add("db_03.user_0099");
        expectedResult.add("db_04.user_0100");
        expectedResult.add("db_05.user_0101");
        expectedResult.add("db_06.user_0102");
        expectedResult.add("db_07.user_0103");
        expectedResult.add("db_00.user_0104");
        expectedResult.add("db_01.user_0105");
        expectedResult.add("db_02.user_0106");
        expectedResult.add("db_03.user_0107");
        expectedResult.add("db_04.user_0108");
        expectedResult.add("db_05.user_0109");
        expectedResult.add("db_06.user_0110");
        expectedResult.add("db_07.user_0111");
        expectedResult.add("db_00.user_0112");
        expectedResult.add("db_01.user_0113");
        expectedResult.add("db_02.user_0114");
        expectedResult.add("db_03.user_0115");
        expectedResult.add("db_04.user_0116");
        expectedResult.add("db_05.user_0117");
        expectedResult.add("db_06.user_0118");
        expectedResult.add("db_07.user_0119");
        expectedResult.add("db_00.user_0120");
        expectedResult.add("db_01.user_0121");
        expectedResult.add("db_02.user_0122");
        expectedResult.add("db_03.user_0123");
        expectedResult.add("db_04.user_0124");
        expectedResult.add("db_05.user_0125");
        expectedResult.add("db_06.user_0126");
        expectedResult.add("db_07.user_0127");
        buildParserForId();
        int count = 0;
        List<RouteInfo> list0 = ShardRouteHelper.getConfiguredRouteInfos("db", "user");
        if (expectedResult != null) {
            for (RouteInfo si : list0) {
                Assert.equals(si.toString(), expectedResult.get(count));
                count++;
            }
        }
    }

}
