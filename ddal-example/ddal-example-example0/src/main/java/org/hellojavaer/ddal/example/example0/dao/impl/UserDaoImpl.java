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
package org.hellojavaer.ddal.example.example0.dao.impl;

import org.hellojavaer.ddal.example.example0.dao.UserDao;
import org.hellojavaer.ddal.example.example0.entity.UserEntity;
import org.hellojavaer.ddal.sequence.Sequence;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 15/06/2017.
 */
@Repository
public class UserDaoImpl implements UserDao {

    @Autowired
    private SqlSessionTemplate sqlSessionTemplate;

    @Autowired
    private Sequence           sequence;

    @Override
    public Long add(UserEntity userEntity) {
        Long id = sequence.nextValue(100, TimeUnit.MILLISECONDS);
        userEntity.setId(id);
        sqlSessionTemplate.insert("user.add", userEntity);
        return id;
    }

    @Override
    public int deleteById(Long id) {
        return sqlSessionTemplate.delete("user.deleteById", id);
    }

    @Override
    public int updateById(UserEntity userEntity) {
        return sqlSessionTemplate.update("user.updateById", userEntity);
    }

    @Override
    public UserEntity getById(Long id) {
        return sqlSessionTemplate.selectOne("user.getById", id);
    }

    @Override
    public List<UserEntity> scanQueryAll() {
        return sqlSessionTemplate.selectList("user.scanQueryAll");
    }

}
