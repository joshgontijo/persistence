///*
// * Copyright 2017 Josue Gontijo
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// *
// */
//
//package io.joshworks.snappy.extensions.mvstore;
//
//import com.google.gson.Gson;
//import org.h2.mvstore.WriteBuffer;
//import org.h2.mvstore.type.DataType;
//import org.h2.mvstore.type.StringDataType;
//
//import java.lang.reflect.Type;
//import java.nio.ByteBuffer;
//
///**
// * Created by Josh Gontijo on 4/22/17.
// */
//public class JsonSerializer implements DataType {
//
//    private final Type type;
//    private static final Gson gson = new Gson();
//
//    private final StringDataType stringDataType = new StringDataType();
//
//    private JsonSerializer(Class<?> type) {
//        this.type = type;
//    }
//
//    private JsonSerializer(Type type) {
//        this.type = type;
//    }
//
//    public static JsonSerializer of(Class<?> type) {
//        return new JsonSerializer(type);
//    }
//
//    public static JsonSerializer of(Type type) {
//        return new JsonSerializer(type);
//    }
//
//    @Override
//    public int compare(Object aObj, Object bObj) {
//        return stringDataType.compare(aObj, bObj);
//    }
//
//    @Override
//    public int getMemory(Object o) {
//        return stringDataType.getMemory(o);
//    }
//
//    @Override
//    public void write(WriteBuffer writeBuffer, Object obj) {
//        String json = gson.toJson(obj);
//        stringDataType.write(writeBuffer, json);
//    }
//
//    @Override
//    public void write(WriteBuffer buff, Object[] obj, int len, boolean key) {
//        for (int i = 0; i < len; i++) {
//            write(buff, obj[i]);
//        }
//    }
//
//    @Override
//    public Object read(ByteBuffer byteBuffer) {
//        String json = stringDataType.read(byteBuffer);
//        return gson.fromJson(json, type);
//    }
//
//    @Override
//    public void read(ByteBuffer var1, Object[] var2, int var3, boolean var4) {
//        for (int var5 = 0; var5 < var3; ++var5) {
//            var2[var5] = this.read(var1);
//        }
//    }
//}
