/*
 * Copyright 2017-2017 the original author or authors.
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
package org.hellojavaer.ddal.example.example0;

import com.alibaba.fastjson.JSON;
import org.hellojavaer.ddal.ddr.cluster.DBClusterRouteContext;
import org.hellojavaer.ddal.ddr.shard.ShardRouteContext;
import org.hellojavaer.ddal.ddr.shard.ShardRouteInfo;
import org.hellojavaer.ddal.ddr.shard.ShardRouteUtils;
import org.hellojavaer.ddal.ddr.shard.ShardRouter;
import org.hellojavaer.ddal.example.example0.dao.UserDao;
import org.hellojavaer.ddal.example.example0.entity.UserEntity;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 19/06/2017.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/context.xml" })
public class UserDaoTest {

    @Autowired
    private UserDao     userDao;

    @Value("#{dataSource.shardRouter}")
    private ShardRouter shardRouter;

    static {
        DBClusterRouteContext.setClusterName("default");
    }

    @Test
    @Transactional
    public void testForCRUD() {
        // insert
        UserEntity userEntity = new UserEntity();
        userEntity.setName("allen");
        Long id = userDao.add(userEntity);
        UserEntity userEntityInDb = userDao.getById(id);
        // check insert operation
        Assert.isTrue(userEntityInDb.getId().equals(userEntity.getId()));
        Assert.isTrue(userEntityInDb.getName().equals(userEntity.getName()));

        // update
        UserEntity userEntityForUpdate = new UserEntity();
        userEntityForUpdate.setId(id);
        userEntityForUpdate.setName("allen_well");
        int rows = userDao.updateById(userEntityForUpdate);
        userEntityInDb = userDao.getById(id);
        // check update operation
        Assert.isTrue(rows == 1);
        Assert.isTrue(userEntityInDb.getId().equals(userEntityForUpdate.getId()));
        Assert.isTrue(userEntityInDb.getName().equals(userEntityForUpdate.getName()));

        // delete
        rows = userDao.deleteById(id);
        userEntityInDb = userDao.getById(id);
        // check delete operation
        Assert.isTrue(rows == 1);
        Assert.isTrue(userEntityInDb == null);
    }

    @Test
    @Transactional
    public void scanQueryAll() {
        // step0: make some test data
        List<UserEntity> userEntities = new ArrayList<>(16);
        for (int i = 0; i < 16; i++) {
            UserEntity userEntity = new UserEntity();
            userEntity.setName(String.valueOf(System.currentTimeMillis()));
            userEntities.add(userEntity);
            userDao.add(userEntity);
        }

        // step1: check scan query
        String scName = "base";
        String tbName = "user";
        List<ShardRouteInfo> routeInfos = shardRouter.getRouteInfos(scName, tbName);
        for (ShardRouteInfo routeInfo : routeInfos) {
            // when sql expression doesn't contain shard value, use ShardRouteContext to set route information
            ShardRouteContext.setRouteInfo(scName, tbName, routeInfo);
            List<UserEntity> userEntities0 = userDao.scanQueryAll();
            System.out.println("====== table: '" + routeInfo.toString() + "' contains the following records ==========");
            if (userEntities0 != null) {
                for (UserEntity item : userEntities0) {
                    System.out.println(item);
                }
            }
            ShardRouteContext.clearContext();
        }

        // step2: remove test
        for (UserEntity userEntity : userEntities) {
            userDao.deleteById(userEntity.getId());
        }
    }

    @Test
    public void groupRouteInfo() {
        List<Long> ids = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            ids.add((long) i);
        }
        Map<ShardRouteInfo, List<Long>> map = ShardRouteUtils.groupSdValuesByRouteInfo(shardRouter, "base", "user", ids);
        for (Map.Entry<ShardRouteInfo, List<Long>> entry : map.entrySet()) {
            System.out.println(entry.getKey() + " -> " + JSON.toJSONString(entry.getValue()));
        }
    }

}
