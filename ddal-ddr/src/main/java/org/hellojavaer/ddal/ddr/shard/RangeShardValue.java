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
package org.hellojavaer.ddal.ddr.shard;

import java.util.Objects;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 16/09/2017.
 */
public class RangeShardValue {

    private Long begin;
    private Long end;

    public RangeShardValue() {
    }

    public RangeShardValue(Long begin, Long end) {
        this.begin = begin;
        this.end = end;
    }

    public Long getBegin() {
        return begin;
    }

    public void setBegin(Long begin) {
        this.begin = begin;
    }

    public Long getEnd() {
        return end;
    }

    public void setEnd(Long end) {
        this.end = end;
    }

    @Override
    public String toString() {
        return "{" + "begin:" + begin + ",end:" + end + '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(begin, end);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RangeShardValue that = (RangeShardValue) o;

        if (begin != null ? !begin.equals(that.begin) : that.begin != null) return false;
        return end != null ? end.equals(that.end) : that.end == null;

    }

}
