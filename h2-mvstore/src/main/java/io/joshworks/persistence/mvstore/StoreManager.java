package io.joshworks.persistence.mvstore;

import org.apache.commons.configuration2.CombinedConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.EnvironmentConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.OffHeapStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.util.Objects;

public class StoreManager {

    private static final Logger logger = LoggerFactory.getLogger(StoreManager.class);

    private static final String PROPERTIES_FILE = "mvstore.properties";

    private static final String KEY = "password";
    private static final String LOCATION = "location";
    private static final String AUTO_COMMIT = "autoCommit";
    private static final String CACHE_SIZE = "cacheSize";
    private static final String OFF_HEAP_MODE = "offHeapMode";
    private static final String READ_ONLY = "readOnly";
    private static final String AUTO_COMMIT_BUFFER_SIZE = "autoCommitBufferSize";

    private static final String DEFAULT_LOCATION = System.getProperty("user.home") + File.separator + ".mvstore";
    private static final String DEFAULT_KEY = "admin";
    private static final boolean DEFAULT_AUTO_COMMIT = true;
    private static final boolean DEFAULT_OFF_HEAP_MODE = false;
    private static final boolean DEFAULT_READ_ONLY = false;
    private static final int DEFAULT_CACHE_SIZE = -1; //not set
    private static final int DEFAULT_AUTO_COMMIT_BUFFER_SIZE = -1; //not set
    private static final String DATABASE_NAME = File.separator + "db.dat";
    private static MVStore store;

    private static final Object LOCK = new Object();

    public static void shutdown() {
        if (store != null) {
            synchronized (LOCK) {
                commitAndClose();
                store = null;
            }
        }
    }

    private static void commitAndClose() {
        if (store != null && !store.isClosed()) {
            store.commit();
            store.close();
        }
    }

    public static void defaultStore(MVStore.Builder mvStore) {
        Objects.requireNonNull(mvStore, "MVStore cannot be null");
        synchronized (LOCK) {
            if(store != null){
                commitAndClose();
                store = mvStore.open();
            }
        }
    }

    private static Configuration getConfig() throws Exception {
        URL resource = Thread.currentThread().getContextClassLoader().getResource(PROPERTIES_FILE);
        if (resource != null) {
            Configurations configurations = new Configurations();
            CombinedConfiguration combined = configurations.combined(resource);
            combined.addConfiguration(new EnvironmentConfiguration());
            return combined;
        }
        return new EnvironmentConfiguration();
    }

    static MVStore getStore() {
        if (store == null) {
            synchronized (LOCK) {
                if (store == null) {
                    store = createStore();
                    Runtime.getRuntime().addShutdownHook(new Thread(StoreManager::shutdown));
                }
                return store;
            }
        }
        return store;
    }

    private static MVStore createStore() {
        try {
            Configuration config = getConfig();
            String location = config.getString(LOCATION, DEFAULT_LOCATION);
            String key = config.getString(KEY, DEFAULT_KEY);
            boolean autoCommit = config.getBoolean(AUTO_COMMIT, DEFAULT_AUTO_COMMIT);
            boolean offHeap = config.getBoolean(OFF_HEAP_MODE, DEFAULT_OFF_HEAP_MODE);
            boolean readOnly = config.getBoolean(READ_ONLY, DEFAULT_READ_ONLY);
            int cacheSize = config.getInt(CACHE_SIZE, DEFAULT_CACHE_SIZE);
            int autoCommitBufferSize = config.getInt(AUTO_COMMIT_BUFFER_SIZE, DEFAULT_AUTO_COMMIT_BUFFER_SIZE);


            location = location.endsWith(File.separator) ? location.substring(0, location.length() - 1) : location;
            createFolder(location);

            MVStore.Builder builder = new MVStore.Builder()
                    .fileName(location + DATABASE_NAME)
                    .encryptionKey(key.toCharArray())
                    .compress();


            if (offHeap) {
                builder.fileStore(new OffHeapStore());
            }
            if (!autoCommit) {
                builder.autoCommitDisabled();
            }
            if (cacheSize >= 0) {
                builder.cacheSize(cacheSize);
            }
            if(readOnly) {
                builder.readOnly();
            }
            if(autoCommitBufferSize >= 0) {
                builder.autoCommitBufferSize(autoCommitBufferSize);
            }
            if(autoCommitBufferSize >= 0) {
                builder.autoCommitBufferSize(autoCommitBufferSize);
            }
            return builder.open();


        } catch (Exception ex) {
            throw new RuntimeException("Could not initialize H2 store", ex);
        }
    }

    private static void createFolder(String dirPath) throws Exception {
        File dataDir = new File(dirPath);
        if (!dataDir.exists()) {
            if (!dataDir.mkdirs()) {
                throw new IllegalStateException("Could not create data directory '" + dirPath + "'");
            }
        }
    }


}
