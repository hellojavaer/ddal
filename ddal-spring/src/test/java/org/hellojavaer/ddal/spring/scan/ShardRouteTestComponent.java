package org.hellojavaer.ddal.spring.scan;

import org.hellojavaer.ddal.ddr.shard.ShardRouteContext;
import org.hellojavaer.ddal.ddr.shard.annotation.ShardRoute;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 22/06/2017.
 */
@Component
public class ShardRouteTestComponent {

    @ShardRoute(scName = "user,shop", sdValue = "{#$0}")
    public void route(Long id) {
        Assert.isTrue(ShardRouteContext.getRouteInfo("user") == id);
        Assert.isTrue(ShardRouteContext.getRouteInfo("shop") == id);
    }
}
