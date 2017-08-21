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

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 21/08/2017.
 */
class LinkedCycleList<T> implements Iterable<T> {

    private final int                   size;
    private final AtomicReference<Node> point = new AtomicReference<>();

    private class Node {

        private Node next;
        private T    value;

        public Node getNext() {
            return next;
        }

        public void setNext(Node next) {
            this.next = next;
        }

        public T getValue() {
            return value;
        }

        public void setValue(T value) {
            this.value = value;
        }
    }

    public LinkedCycleList(T... objs) {
        if (objs == null || objs.length == 0) {
            throw new IllegalArgumentException("objs can't be empty");
        }
        Node header = new Node();
        Node tail = header;
        for (T obj : objs) {
            Node node = new Node();
            node.setValue(obj);
            tail.setNext(node);
            tail = node;
        }
        tail.setNext(header.getNext());
        this.point.set(tail.getNext());
        this.size = objs.length;
    }

    public LinkedCycleList(Collection<T> collection) {
        if (collection == null || collection.isEmpty()) {
            throw new IllegalArgumentException("collection can't be empty");
        }
        Node header = new Node();
        Node tail = header;
        for (T obj : collection) {
            Node node = new Node();
            node.setValue(obj);
            tail.setNext(node);
            tail = node;
        }
        tail.setNext(header.getNext());
        this.point.set(tail.getNext());
        this.size = collection.size();
    }

    public T next() {
        if (this.size == 1) {
            return point.get().getValue();
        } else {
            while (true) {
                Node node = point.get();
                Node next = node.getNext();
                if (point.compareAndSet(node, next)) {
                    return next.getValue();
                }
            }
        }
    }

    public int size() {
        return this.size;
    }

    @Override
    public Iterator<T> iterator() {

        return new Iterator<T>() {

            private int  count = size;
            private Node node  = point.get();

            @Override
            public boolean hasNext() {
                return count > 0;
            }

            @Override
            public T next() {
                if (count <= 0) {
                    return null;
                } else {
                    count--;
                    T value = node.getValue();
                    node = node.getNext();
                    return value;
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove");
            }
        };
    }
}
