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
package org.hellojavaer.ddal.spring.scan.dbcluster;

import org.hellojavaer.ddal.core.utils.Assert;
import org.hellojavaer.ddal.ddr.cluster.DBClusterRoute;
import org.hellojavaer.ddal.ddr.cluster.DBClusterRouteContext;
import org.hellojavaer.ddal.spring.scan.UserEntity;
import org.springframework.stereotype.Component;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 2018/5/28.
 */
@Component
public class DBClusterRouteTestComponent {

    @DBClusterRoute(clusterName = "-name-")
    public void test00() {
        Assert.equals(DBClusterRouteContext.getClusterName(), "-name-");
    }

    @DBClusterRoute(clusterName = "-{$0}-")
    public void test01(String str) {
        Assert.equals(DBClusterRouteContext.getClusterName(), "-" + str + "-");
    }

    @DBClusterRoute(clusterName = "{$0}")
    public void test02(int id) {
        Assert.equals(DBClusterRouteContext.getClusterName(), id + "");
    }

    @DBClusterRoute(clusterName = "tb_{format('%04d',$0.id)}")
    public void testFunction(UserEntity userEntity) {
        Assert.equals(DBClusterRouteContext.getClusterName(), (String.format("tb_%04d", userEntity.getId())));

    }

}
