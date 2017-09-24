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
package org.hellojavaer.ddal.ddr.shard;

import org.hellojavaer.ddal.core.utils.Assert;
import org.hellojavaer.ddal.ddr.datasource.exception.AmbiguousDataSourceBindingException;
import org.junit.Test;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 18/12/2016.
 */
public class ShardRouteContextTest {

    /**
     * 一个context 单个绑定
     */
    @Test
    public void test01() {
        ShardRouteContext.clearContext();
        try {
            ShardRouteContext.setRouteInfo("sc", "tb", 2);
            Assert.isTrue(ShardRouteContext.getRouteInfo("sc", "tb") == 2);
            Assert.isTrue(ShardRouteContext.getRouteInfo(null, "tb") == 2);
            Assert.isTrue(ShardRouteContext.containsRouteInfo("sc", "tb"));

            Assert.isTrue(ShardRouteContext.removeRouteInfo("sc", "tb") == 2);
            Assert.isTrue(ShardRouteContext.getRouteInfo("sc", "tb") == null);
            Assert.isTrue(ShardRouteContext.getRouteInfo(null, "tb") == null);
            Assert.isTrue(ShardRouteContext.containsRouteInfo("sc", "tb") == false);
        } finally {
            ShardRouteContext.clearContext();
        }
    }

    /**
     * 一个context多个绑定的情况
     */
    @Test
    public void test02() {
        ShardRouteContext.clearContext();
        try {
            ShardRouteContext.setRouteInfo("sc_00", "tb", 2);
            ShardRouteContext.setRouteInfo("sc_01", "tb", 3);
            Assert.isTrue(ShardRouteContext.containsRouteInfo("sc_00", "tb"));
            Assert.isTrue(ShardRouteContext.getRouteInfo("sc_00", "tb") == 2);
            Assert.isTrue(ShardRouteContext.containsRouteInfo("sc_01", "tb"));
            Assert.isTrue(ShardRouteContext.getRouteInfo("sc_01", "tb") == 3);
            try {
                ShardRouteContext.getRouteInfo(null, "tb");
                throw new Error();
            } catch (AmbiguousDataSourceBindingException e) {
            }
            //
            Assert.isTrue(ShardRouteContext.removeRouteInfo("sc_00", "tb") == 2);
            Assert.isTrue(ShardRouteContext.getRouteInfo(null, "tb") == 3);
        } finally {
            ShardRouteContext.clearContext();
        }
    }

    /**
     * 一个context内 default test
     */
    @Test
    public void test03() {
        ShardRouteContext.clearContext();
        try {
            // add
            ShardRouteContext.setRouteInfo("sc", 2);
            Assert.isTrue(ShardRouteContext.getRouteInfo("sc") == 2);
            Assert.isTrue(ShardRouteContext.containsRouteInfo("sc"));
            Assert.isTrue(ShardRouteContext.getRouteInfo("sc", "anything") == 2);
            Assert.isTrue(ShardRouteContext.containsRouteInfo("sc", "anything") == false);

            // remove
            Assert.isTrue(ShardRouteContext.removeRouteInfo("sc") == 2);
            Assert.isTrue(ShardRouteContext.getRouteInfo("sc") == null);
            Assert.isTrue(ShardRouteContext.containsRouteInfo("sc") == false);
            Assert.isTrue(ShardRouteContext.getRouteInfo("sc", "anything") == null);
            Assert.isTrue(ShardRouteContext.containsRouteInfo("sc", "anything") == false);
        } finally {
            ShardRouteContext.clearContext();
        }
    }

    /**
     * 多个context 不使用default
     */
    @Test
    public void test10() {
        ShardRouteContext.clearContext();
        try {
            // 继承
            ShardRouteContext.setRouteInfo("sc", "tb", 2);
            ShardRouteContext.pushContext();
            Assert.isTrue(ShardRouteContext.getRouteInfo("sc", "tb") == 2);
            Assert.isTrue(ShardRouteContext.getRouteInfo(null, "tb") == 2);
            Assert.isTrue(ShardRouteContext.containsRouteInfo("sc", "tb") == false);
            ShardRouteContext.setRouteInfo("sc_01", "tb", 3);
            Assert.isTrue(ShardRouteContext.getRouteInfo("sc", "tb") == 2);
            Assert.isTrue(ShardRouteContext.getRouteInfo(null, "tb") == 3);

            // 删除上一级context内容测试
            Assert.isTrue(ShardRouteContext.removeRouteInfo("sc", "tb") == null);

            // 覆盖测试
            ShardRouteContext.setRouteInfo("sc", "tb", null);
            Assert.isTrue(ShardRouteContext.removeRouteInfo("sc_01", "tb") == 3);
            Assert.isTrue(ShardRouteContext.getRouteInfo(null, "tb") == null);

            ShardRouteContext.popContext();
            Assert.isTrue(ShardRouteContext.removeRouteInfo("sc", "tb") == 2);
        } finally {
            ShardRouteContext.clearContext();
        }
    }

    /**
     * 多个context 使用default
     */
    @Test
    public void test11() {
        ShardRouteContext.clearContext();
        try {
            // 继承
            ShardRouteContext.setRouteInfo("sc", 1);

            ShardRouteContext.pushContext();
            Assert.isTrue(ShardRouteContext.getRouteInfo("sc", "tb") == 1);
            Assert.isTrue(ShardRouteContext.containsRouteInfo("sc", "tb") == false);
            Assert.isTrue(ShardRouteContext.containsRouteInfo("sc") == false);

            ShardRouteContext.setRouteInfo("sc", "tb", 2);
            Assert.isTrue(ShardRouteContext.getRouteInfo("sc", "tb") == 2);
            Assert.isTrue(ShardRouteContext.containsRouteInfo("sc", "tb") == true);
            Assert.isTrue(ShardRouteContext.containsRouteInfo("sc") == false);

            ShardRouteContext.popContext();
            Assert.isTrue(ShardRouteContext.removeRouteInfo("sc") == 1);
        } finally {
            ShardRouteContext.clearContext();
        }
    }

    /**
     * 
     */
    @Test
    public void test20() {
        // root context can't be pop
        try {
            ShardRouteContext.popContext();
            throw new Error();
        } catch (IndexOutOfBoundsException e) {
        }
        //
        ShardRouteContext.pushContext();
        ShardRouteContext.pushContext();
        ShardRouteContext.popContext();
        ShardRouteContext.popContext();
        try {
            ShardRouteContext.popContext();
            throw new Error();
        } catch (IndexOutOfBoundsException e) {
        }
    }
}
