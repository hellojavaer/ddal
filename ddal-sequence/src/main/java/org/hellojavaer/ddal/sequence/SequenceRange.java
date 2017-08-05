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
package org.hellojavaer.ddal.sequence;

import java.io.Serializable;

/**
 * [begin,end] closed interval
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 04/01/2017.
 */
public class SequenceRange implements Serializable {

    private static final long serialVersionUID = 0L;

    private long              beginValue;
    private long              endValue;

    public SequenceRange() {
    }

    public SequenceRange(long beginValue, long endValue) {
        this.beginValue = beginValue;
        this.endValue = endValue;
    }

    public long getBeginValue() {
        return beginValue;
    }

    public void setBeginValue(long beginValue) {
        this.beginValue = beginValue;
    }

    public long getEndValue() {
        return endValue;
    }

    public void setEndValue(long endValue) {
        this.endValue = endValue;
    }

    @Override
    public String toString() {
        return new StringBuilder().append("{beginValue:").append(beginValue).append(",endValue:").append(endValue).append('}').toString();
    }
}
