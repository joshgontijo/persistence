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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInputStream;
import com.esotericsoftware.kryo.io.ByteBufferOutputStream;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import de.javakaffee.kryoserializers.ArraysAsListSerializer;
import de.javakaffee.kryoserializers.GregorianCalendarSerializer;
import de.javakaffee.kryoserializers.JdkProxySerializer;
import de.javakaffee.kryoserializers.SynchronizedCollectionsSerializer;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.DataType;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.lang.reflect.InvocationHandler;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.GregorianCalendar;
import java.util.Objects;

/**
 * Created by Josh Gontijo on 4/22/17.
 */
class H2KryoSerializer implements DataType {

    private final Kryo kryo = newKryoInstance();
    private final Class<?> type;

    H2KryoSerializer(Class<?> type) {
        this.type = type;
    }

    H2KryoSerializer() {
        this.type = null;
    }

    private static Kryo newKryoInstance() {
        Kryo kryo = new Kryo();
        kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
        kryo.register(Arrays.asList("").getClass(), new ArraysAsListSerializer());
        kryo.register(Collections.EMPTY_LIST.getClass(), new DefaultSerializers.CollectionsEmptyListSerializer());
        kryo.register(Collections.EMPTY_MAP.getClass(), new DefaultSerializers.CollectionsEmptyMapSerializer());
        kryo.register(Collections.EMPTY_SET.getClass(), new DefaultSerializers.CollectionsEmptySetSerializer());
        kryo.register(Collections.singletonList("").getClass(), new DefaultSerializers.CollectionsSingletonListSerializer());
        kryo.register(Collections.singleton("").getClass(), new DefaultSerializers.CollectionsSingletonSetSerializer());
        kryo.register(Collections.singletonMap("", "").getClass(), new DefaultSerializers.CollectionsSingletonMapSerializer());
        kryo.register(GregorianCalendar.class, new GregorianCalendarSerializer());
        kryo.register(InvocationHandler.class, new JdkProxySerializer());
        UnmodifiableCollectionsSerializer.registerSerializers(kryo);
        SynchronizedCollectionsSerializer.registerSerializers(kryo);
        return kryo;
    }

    @Override
    public int compare(Object aObj, Object bObj) {
        return Objects.compare(aObj, bObj, Comparator.comparing(Object::hashCode));
    }

    @Override
    public int getMemory(Object obj) {
        if (obj == null) {
            return 0;
        }
        return 24 + 2 * obj.toString().length();
    }

    @Override
    public void write(WriteBuffer writeBuffer, Object obj) {
        try (Output output = new Output(new ByteBufferOutputStream(writeBuffer.getBuffer()))) {
            if (type == null) {
                kryo.writeClassAndObject(output, obj);
            } else {
                kryo.writeObject(output, obj);
            }
        }
    }

    @Override
    public void write(WriteBuffer buff, Object[] obj, int len, boolean key) {
        for (int i = 0; i < len; i++) {
            write(buff, obj[i]);
        }
    }

    @Override
    public Object read(ByteBuffer byteBuffer) {
        try (Input input = new Input(new ByteBufferInputStream(byteBuffer))) {
            return type == null ? kryo.readClassAndObject(input) : kryo.readObject(input, type);
        }
    }

    @Override
    public void read(ByteBuffer var1, Object[] var2, int var3, boolean var4) {
        for (int var5 = 0; var5 < var3; ++var5) {
            var2[var5] = this.read(var1);
        }
    }
}
