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

package io.joshworks.snappy.extensions.cblite;

import com.couchbase.lite.Database;
import com.couchbase.lite.DatabaseOptions;
import com.couchbase.lite.Document;
import com.couchbase.lite.Manager;
import com.couchbase.lite.Query;
import com.couchbase.lite.QueryEnumerator;
import com.couchbase.lite.QueryRow;
import com.couchbase.lite.SavedRevision;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.configuration2.CombinedConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.EnvironmentConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;

import java.io.Closeable;
import java.io.File;
import java.lang.reflect.Type;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Josh Gontijo on 3/28/17.
 */
public class CouchbaseStore<T> implements Closeable {

    private static final String PROPERTIES_FILE = "cblite.properties";

    private final Manager manager;
    private final DatabaseOptions options;
    private static final Gson gson = new Gson();

    private static final String KEY = "password";
    private static final String LOCATION = "location";
    private static final String DEFAULT_LOCATION = System.getProperty("user.home") + File.separator + "snappy" + File.separator + "cblite";
    private static final String DEFAULT_KEY = "admin";

    private final Class<T> type;
    private final String databaseName;

    private CouchbaseStore(String databaseName, Class<T> type) {
        this.databaseName = databaseName;
        this.type = type;

        try {
            Configuration config = getConfig();
            String location = config.getString(LOCATION, DEFAULT_LOCATION);
            String key = config.getString(KEY, DEFAULT_KEY);

            options = new DatabaseOptions();
            options.setCreate(true);
            options.setEncryptionKey(key);

            manager = new Manager(new StoreContext(location), Manager.DEFAULT_OPTIONS);
            StoreManager.add(manager);

        } catch (Exception e) {
            throw new RuntimeException("Could not initialize Couchbase store", e);
        }
    }

    private Configuration getConfig() throws Exception {
        URL resource = Thread.currentThread().getContextClassLoader().getResource(PROPERTIES_FILE);
        if (resource != null) {
            Configurations configurations = new Configurations();
            CombinedConfiguration combined = configurations.combined(resource);
            combined.addConfiguration(new EnvironmentConfiguration());
            return combined;
        }
        return new EnvironmentConfiguration();
    }

    public static <T> CouchbaseStore<T> of(String name, Class<T> type) {
        return new CouchbaseStore<>(name, type);
    }

    public Database getDatabase(String name) {
        try {
            return manager.openDatabase(name, options);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Manager getManager() {
        return manager;
    }

    public void create(String id, T object) {
        try {
            Map<String, Object> objectMap = toJsonMap(object);
            Database database = getDatabase(databaseName);
            Document document = database.getDocument(id);
            document.putProperties(objectMap);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void update(String id, T object) {
        try {
            Database database = getDatabase(databaseName);
            Document document = database.getDocument(id);
            Map<String, Object> properties = new HashMap<>(document.getProperties());

            Map<String, Object> objectMap = toJsonMap(object);
            properties.putAll(objectMap);

            document.putProperties(properties);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public Map<String, T> all() {
        try {
            Map<String, T> results = new HashMap<>();
            Query query = getDatabase(databaseName).createAllDocumentsQuery();
            QueryEnumerator result = query.run();
            while (result.hasNext()) {
                QueryRow row = result.next();
                results.put(row.getDocumentId(), fromJson(row.getDocument()));
            }
            return results;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public T get(String id) {
        Document doc = getDatabase(databaseName).getDocument(id);
        return fromJson(doc);
    }

    public SavedRevision merge(String id, Map<String, Object> newValues) {
        try {
            Document doc = getDatabase(databaseName).getDocument(id);
            return doc.update(newRevision -> {
                Map<String, Object> properties = newRevision.getUserProperties();
                properties.putAll(newValues);
                newRevision.setUserProperties(properties);
                return true;
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean delete(String id) {
        try {
            Document doc = getDatabase(databaseName).getDocument(id);
            return doc.delete();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private T fromJson(Document doc) {
        Map<String, Object> properties = doc.getProperties();
        JsonElement jsonElement = gson.toJsonTree(properties);
        return gson.fromJson(jsonElement, type);
    }

    private Map<String, Object> toJsonMap(T data) {
        Type type = new TypeToken<Map<String, Object>>() {
        }.getType();
        return gson.fromJson(gson.toJson(data), type);
    }

    @Override
    public void close() {
        if (manager != null) {
            manager.close();
        }
    }
}
