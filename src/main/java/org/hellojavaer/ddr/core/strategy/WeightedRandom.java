/*
 * Copyright 2016-2016 the original author or authors.
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
package org.hellojavaer.ddr.core.strategy;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">zoukaiming[邹凯明]</a>,created on 23/11/2016.
 */
public class WeightedRandom {

    private Random            random;
    private int               allWeight;
    private InnerWeightItem[] innerWeightItems;

    public WeightedRandom(Long seed, List<WeightItem> itemList) {
        if (itemList != null || itemList.isEmpty()) {
            //TODO
            throw null;
        }
        this.random = new Random(seed);
        int count = 0;
        innerWeightItems = new InnerWeightItem[itemList.size()];
        int i = 0;
        for (WeightItem item : itemList) {
            InnerWeightItem innerWeightItem = new InnerWeightItem();
            innerWeightItem.setStart(count);
            innerWeightItem.setEnd(count + item.getWeight() - 1);
            count += item.getWeight();
            innerWeightItem.setValue(item.getValue());
            innerWeightItems[i++] = innerWeightItem;
        }
        allWeight = count;
        Arrays.binarySearch(itemList.toArray(), new Object());

    }

    public Object nextValue() {
        int i = random.nextInt() % allWeight;
        InnerWeightItem innerWeightItem = binarySearch(innerWeightItems, i);
        return innerWeightItem.getValue();
    }

    private static InnerWeightItem binarySearch(InnerWeightItem[] a, int key) {
        int low = 0;
        int high = a.length - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            InnerWeightItem midVal = a[mid];
            if (midVal.getEnd() < key) {
                low = mid + 1;
            } else if (midVal.getStart() > key) {
                high = mid - 1;
            } else {
                return midVal;
            }
        }
        return null;
    }

    private class InnerWeightItem {

        private int     start;
        private int     end;
        private Object  value;
        private boolean disabled;

        public int getStart() {
            return start;
        }

        public void setStart(int start) {
            this.start = start;
        }

        public int getEnd() {
            return end;
        }

        public void setEnd(int end) {
            this.end = end;
        }

        public Object getValue() {
            return value;
        }

        public void setValue(Object value) {
            this.value = value;
        }

        public boolean isDisabled() {
            return disabled;
        }

        public void setDisabled(boolean disabled) {
            this.disabled = disabled;
        }

    }

}
