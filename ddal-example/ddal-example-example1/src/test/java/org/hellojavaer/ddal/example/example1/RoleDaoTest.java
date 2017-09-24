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
package org.hellojavaer.ddal.example.example1;

import org.hellojavaer.ddal.example.example1.dao.RoleDao;
import org.hellojavaer.ddal.example.example1.entitry.RoleEntity;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 22/07/2017.
 */
@Component
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/context.xml" })
public class RoleDaoTest {

    @Autowired
    private RoleDao roleDao;

    @Autowired
    private ApplicationContext context;

    @Test
    @Transactional
    public void testForCRUD() {
        // 1. do insert
        RoleEntity roleEntity = new RoleEntity();
        roleEntity.setName("role_name");
        Long id = roleDao.add(roleEntity);

        // check insert
        RoleEntity roleInDB = roleDao.getById(id);
        Assert.isTrue(roleInDB != null && roleInDB.getName().equals(roleEntity.getName()));

        // 2. do update
        RoleEntity roleForUpdate = new RoleEntity();
        roleForUpdate.setId(id);
        roleForUpdate.setName("updated_role_name");
        int rows = roleDao.updateById(roleForUpdate);
        // check update
        Assert.isTrue(rows == 1);
        roleInDB = roleDao.getById(id);
        Assert.isTrue(roleInDB != null && roleInDB.getName().equals(roleForUpdate.getName()));

        // 3. do delete
        rows = roleDao.deleteById(id);
        // check delete
        Assert.isTrue(rows == 1);
        roleInDB = roleDao.getById(id);
        Assert.isTrue(roleInDB == null);
    }

    /**
     * after you run this method, you can see the log at the console to check database invoking records.
     */
    @Test
    public void testLoadBalance() {
        for (int i = 0; i < 20; i++) {
            context.getBean(RoleDaoTest.class).getById(1L);
        }
    }

    @Transactional(readOnly = true)
    public RoleEntity getById(Long id) {
        return roleDao.getById(id);
    }

}
