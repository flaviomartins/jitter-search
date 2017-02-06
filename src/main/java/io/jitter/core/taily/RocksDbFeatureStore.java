package io.jitter.core.taily;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;

public class RocksDbFeatureStore implements FeatureStore {
    private static final Logger logger = LoggerFactory.getLogger(RocksDbFeatureStore.class);

    private final Options options;
    private RocksDB db;

    public RocksDbFeatureStore(String dir, boolean readOnly) {
        // the Options class contains a set of configurable DB options
        // that determines the behavior of a database.
        options = new Options();

        try {
            File dirFile = new File(dir);
            boolean isDirectory = dirFile.isDirectory();
            if (!isDirectory) {
                isDirectory = dirFile.mkdirs();
            }

            // FIXME: Should we do something else here?
            if (!isDirectory) {
                throw new RuntimeException("Cannot create directory " + dirFile.getAbsolutePath());
            }

            // a factory method that returns a RocksDB instance
            if (readOnly) {
                db = RocksDB.openReadOnly(options, dir);
            } else {
                options.setCreateIfMissing(true);
                db = RocksDB.open(options, dir);
            }
            // do something
        } catch (RocksDBException rdbe) {
            // do some error handling
            logger.error(rdbe.getMessage());
            options.close();
        }
    }

    @Override
    public void close() {
        if (db != null) {
            db.close();
        }
        options.close();
    }

    @Override
    public double getFeature(String keyStr) {
        if (db == null) {
            return -1;
        }

        byte[] key = keyStr.getBytes(UTF_8);
        try {
            byte[] bytes = db.get(key);
            if (bytes != null) {
                return ByteBuffer.wrap(bytes).getDouble();
            } else {
                return -1;
            }
        } catch (RocksDBException e) {
            return -1;
        }
    }

    @Override
    public void putFeature(String keyStr, double val, long frequency) {
        byte[] key = keyStr.getBytes(UTF_8);
        byte[] bytes = new byte[8];
        ByteBuffer.wrap(bytes).putDouble(val);
        try {
            db.put(key, bytes);
        } catch (RocksDBException e) {
            // error handling
        }

    }

    @Override
    public void addValFeature(String keyStr, double val, long frequency) {
        if (db == null) {
            return;
        }

        byte[] key = keyStr.getBytes(UTF_8);
        double prevVal;
        try {
            byte[] bytes = db.get(key);
            if (bytes != null) {
                prevVal = ByteBuffer.wrap(bytes).getDouble();
            } else {
                prevVal = 0;
            }
        } catch (RocksDBException e) {
            prevVal = 0;
        }

        putFeature(keyStr, val + prevVal, frequency);
    }
}
