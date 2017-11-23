/*
 * Copyright 2017 Josue Gontijo
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.joshworks.persistence.mvstore;

import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.type.DataType;
import org.h2.mvstore.type.ObjectDataType;
import org.h2.mvstore.type.StringDataType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Date;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;

/**
 * Created by Josh Gontijo on 3/28/17.
 */
public class LocalStore {

    private static final Set<Class<?>> defaultTypes = new HashSet<>(Arrays.asList(
            Integer.class,
            Byte.class,
            Short.class,
            Character.class,
            Long.class,
            Float.class,
            BigInteger.class,
            BigDecimal.class,
            Double.class,
            UUID.class,
            Date.class));


    private LocalStore() {
    }

    public static <K, V> Map<K, V> of(String name, Class<K> keyType, Class<V> valueType) {
        return StoreManager.getStore().openMap(name, new MVMap.Builder<K, V>()
                .keyType(getDataType(keyType))
                .valueType(getDataType(valueType)));
    }

    public static <K, V> Map<K, List<V>> listOf(String name, Class<K> keyType, Class<V> valueType) {
        return StoreManager.getStore().openMap(name, new MVMap.Builder<K, List<V>>()
                .keyType(getDataType(keyType))
                .valueType(new H2KryoSerializer()));
    }

    public static <K, V> Map<K, Set<V>> setOf(String name, Class<K> keyType, Class<V> valueType) {
        return StoreManager.getStore().openMap(name, new MVMap.Builder<K, Set<V>>()
                .keyType(getDataType(keyType))
                .valueType(new H2KryoSerializer()));
    }

    public static <K, V> Map<K, Queue<V>> queueOf(String name, Class<K> keyType, Class<V> valueType) {
        return StoreManager.getStore().openMap(name, new MVMap.Builder<K, Queue<V>>()
                .keyType(getDataType(keyType))
                .valueType(new H2KryoSerializer()));
    }

    public static <K, V> Map<K, Deque<V>> dequeOf(String name, Class<K> keyType, Class<V> valueType) {
        return StoreManager.getStore().openMap(name, new MVMap.Builder<K, Deque<V>>()
                .keyType(getDataType(keyType))
                .valueType(new H2KryoSerializer()));
    }

    public static <K, V> Map<K, V> of(String name, Class<K> keyType, TypeReference<V> valueType) {
        return StoreManager.getStore().openMap(name, new MVMap.Builder<K, V>()
                .keyType(getDataType(keyType))
                .valueType(new H2KryoSerializer()));
    }

    public static void close() {
        StoreManager.shutdown();
    }

    public static MVStore store() {
        return StoreManager.getStore();
    }

    private static DataType getDataType(Class<?> type) {
        if (String.class.equals(type)) {
            return new StringDataType();
        }
        if (defaultTypes.contains(type)) {
            return new ObjectDataType();
        }

        return new H2KryoSerializer(type);
    }



}
