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
package org.hellojavaer.ddal.example.example1.dao;

import org.hellojavaer.ddal.example.example1.entitry.RoleEntity;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 22/07/2017.
 */
public interface RoleDao {

    /**
     * 
     * @param roleEntity
     * @return
     */
    Long add(RoleEntity roleEntity);

    /**
     *
     * @param id
     * @return effective lines
     */
    int deleteById(Long id);

    /**
     * 
     * @param roleEntity
     * @return
     */
    int updateById(RoleEntity roleEntity);

    /**
     *
     * @param id
     * @return
     */
    RoleEntity getById(Long id);

}
