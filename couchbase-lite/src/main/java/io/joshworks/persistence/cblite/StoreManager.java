package io.joshworks.persistence.cblite;

import com.couchbase.lite.Manager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class StoreManager {

    private static final Logger logger = LoggerFactory.getLogger(StoreManager.class);
    private static final Map<String, Manager> stores = new ConcurrentHashMap<>();

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(StoreManager::shutdown));
    }

    static void add(Manager store) {
        String uuid = UUID.randomUUID().toString().substring(0, 8);
        stores.put(uuid, store);
    }

    public static void shutdown() {
        for (Map.Entry<String, Manager> kv : stores.entrySet()) {
            try {
                logger.info("Closing store {}", kv.getKey());
                kv.getValue().close();
            } catch (Exception e) {
                logger.error("Error while closing store " + kv.getKey(), e);
            }
        }
    }


}
