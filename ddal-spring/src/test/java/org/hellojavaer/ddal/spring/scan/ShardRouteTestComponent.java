/*
 * Copyright 2016-2017 the original author or authors.
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
package org.hellojavaer.ddal.spring.scan;

import org.hellojavaer.ddal.core.utils.Assert;
import org.hellojavaer.ddal.ddr.shard.ShardRoute;
import org.hellojavaer.ddal.ddr.shard.ShardRouteContext;
import org.springframework.stereotype.Component;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 22/06/2017.
 */
@Component
public class ShardRouteTestComponent {

    @ShardRoute(scName = "user,shop", sdValue = "{$0}")
    public void routeWithId(Long id) {
        Assert.isTrue(ShardRouteContext.getRouteInfo("user") == id);
        Assert.isTrue(ShardRouteContext.getRouteInfo("shop") == id);
    }

    @ShardRoute(scName = "user,shop", sdValue = "{$0.id}")
    public void routeWithEntity(UserEntity userEntity) {
        Assert.isTrue(ShardRouteContext.getRouteInfo("user") == userEntity.getId());
        Assert.isTrue(ShardRouteContext.getRouteInfo("shop") == userEntity.getId());
    }

    @ShardRoute(scName = "user,shop", sdValue = "tb_{format('%04d',$0.id)}")
    public void testFunction(UserEntity userEntity) {
        Assert.isTrue(ShardRouteContext.getRouteInfo("user").equals(String.format("tb_%04d", userEntity.getId())));
        Assert.isTrue(ShardRouteContext.getRouteInfo("shop").equals(String.format("tb_%04d", userEntity.getId())));
    }
}
