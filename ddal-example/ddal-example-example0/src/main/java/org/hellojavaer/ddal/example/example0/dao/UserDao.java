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
package org.hellojavaer.ddal.example.example0.dao;

import org.hellojavaer.ddal.example.example0.entity.UserEntity;

import java.util.List;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 15/06/2017.
 */
public interface UserDao {

    /**
     * @param userEntity
     * @return id
     */
    Long add(UserEntity userEntity);

    /**
     * 
     * @param id
     * @return effective lines
     */
    int deleteById(Long id);

    /**
     * 
     * @param userEntity
     * @return effective lines
     */
    int updateById(UserEntity userEntity);

    /**
     * 
     * @param id
     * @return
     */
    UserEntity getById(Long id);

    /**
     * 
     * @return
     */
    List<UserEntity> scanQueryAll();
}
