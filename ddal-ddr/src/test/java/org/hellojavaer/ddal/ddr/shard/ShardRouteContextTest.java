package org.hellojavaer.ddal.ddr.shard;

import org.hellojavaer.ddal.ddr.utils.Assert;
import org.junit.Test;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 18/12/2016.
 */
public class ShardRouteContextTest {

    @Test
    public void test00() {
        ShardRouteContext.clear();
        try {
            ShardRouteContext.setRouteInfo("db", "user", 1);
            Assert.isTrue(ShardRouteContext.getRouteInfo(null, "user") == 1);
            ShardRouteContext.setRouteInfo("shop", "user", 2);
            Assert.isTrue(ShardRouteContext.getRouteInfo("shop", "user") == 2);
        } finally {
            ShardRouteContext.clear();
        }
    }

    @Test
    public void test01() {
        ShardRouteContext.clear();
        try {
            ShardRouteContext.setRouteInfo("db", "user", 1);
            ShardRouteContext.pushSubContext();
            ShardRouteContext.setRouteInfo("shop", "user", 2);
            ShardRouteContext.setRouteInfo("shop", "user", 3);
            Assert.isTrue(ShardRouteContext.getRouteInfo(null, "user") == 3);
            ShardRouteContext.setRouteInfo("shop", "user", null);
            ShardRouteContext.setRouteInfo("shop", "user", null);
            Assert.isTrue(ShardRouteContext.getRouteInfo(null, "user") == null);
            ShardRouteContext.popSubContext();
            Assert.isTrue(ShardRouteContext.getRouteInfo(null, "user") == 1);
            try {
                ShardRouteContext.popSubContext();
                Assert.isTrue(false);
            } catch (IndexOutOfBoundsException e) {
            }
            ShardRouteContext.clear();
            Assert.isTrue(ShardRouteContext.getRouteInfo(null, "user") == null);
        } finally {
            ShardRouteContext.clear();
        }
    }

    @Test
    public void test02() {
        ShardRouteContext.clear();
        try {
            ShardRouteContext.setRouteInfo("db", "user", 1);
            ShardRouteContext.setRouteInfo("shop", "user", 2);
            ShardRouteContext.removeRouteInfo("shop", "user");
            Assert.isTrue(ShardRouteContext.getRouteInfo(null, "user") == 1);
        } finally {
            ShardRouteContext.clear();
        }
    }
}
