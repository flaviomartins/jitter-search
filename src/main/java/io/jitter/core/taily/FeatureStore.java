package io.jitter.core.taily;

import com.sleepycat.bind.tuple.DoubleBinding;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class FeatureStore {
    private static final Logger logger = LoggerFactory.getLogger(FeatureStore.class);

    public static final String FEAT_SUFFIX = "#f";
    public static final String SQUARED_FEAT_SUFFIX = "#f2";
    public static final String MIN_FEAT_SUFFIX = "#m";
    public static final String SIZE_FEAT_SUFFIX = "#d";
    public static final String TERM_SIZE_FEAT_SUFFIX = "#t";

    private static final String FREQ_DB_NAME = "freq.db";
    private static final String INFREQ_DB_NAME = "infreq.db";
    public static final int FREQUENT_TERMS = 1000; // tf required for a term to be considered "frequent"

    private Environment dbEnv;
    private final Database freqDb; // db storing frequent terms; see FREQUENT_TERMS
    private final Database infreqDb; // db storing frequent terms; see FREQUENT_TERMS

    public FeatureStore(String dir, boolean readOnly) {
        String freqPath = dir + File.separator + FREQ_DB_NAME;
        String infreqPath = dir + File.separator + INFREQ_DB_NAME;

        try {
            // Instantiate an environment configuration object
            EnvironmentConfig envConfig = new EnvironmentConfig();
            // Configure the environment for the read-only state as identified
            // by the readOnly parameter on this method call.
            envConfig.setReadOnly(readOnly);
            // If the environment is opened for write, then we want to be
            // able to create the environment if it does not exist.
            envConfig.setAllowCreate(!readOnly);

            // Set cache mode
            envConfig.setCacheMode(CacheMode.EVICT_LN);

            File dirFile = new File(dir);
            boolean isDirectory = dirFile.isDirectory();
            if (!isDirectory) {
                isDirectory = dirFile.mkdirs();
            }

            // FIXME: Should we do something else here?
            if (!isDirectory) {
                throw new RuntimeException("Cannot create directory " + dirFile.getAbsolutePath());
            }

            // Instantiate the Environment. This opens it and also possibly
            // creates it.
            dbEnv = new Environment(dirFile, envConfig);
        } catch (DatabaseException dbe) {
            logger.error(dbe.getMessage());
        }

        if (!readOnly) {
            truncateDb(freqPath);
            truncateDb(infreqPath);
        }

        freqDb = openDb(freqPath, readOnly);
        infreqDb = openDb(infreqPath, readOnly);
    }

    public void close() {
        closeDb(freqDb);
        closeDb(infreqDb);
        closeEnv(dbEnv);
    }

    // returns feature; if feature isn't found, returns -1
    public double getFeature(String keyStr) {
        // key
        DatabaseEntry key = new DatabaseEntry();
        // data
        DatabaseEntry data = new DatabaseEntry();

        StringBinding.stringToEntry(keyStr, key);

        OperationStatus status = freqDb.get(null, key, data, LockMode.DEFAULT);

        if (status == OperationStatus.NOTFOUND) {
            status = infreqDb.get(null, key, data, LockMode.DEFAULT);
            if (status == OperationStatus.NOTFOUND) {
                return -1;
            }
        }

        return DoubleBinding.entryToDouble(data);
    }

    public void putFeature(String keyStr, double val, long frequency) {
        // key
        DatabaseEntry key = new DatabaseEntry();
        // data
        DatabaseEntry data = new DatabaseEntry();

        StringBinding.stringToEntry(keyStr, key);
        DoubleBinding.doubleToEntry(val, data);

        Database db;
        if (frequency >= FREQUENT_TERMS) {
            db = freqDb;
        } else {
            db = infreqDb;
        }

        OperationStatus status = db.put(null, key, data);
        if (status == OperationStatus.KEYEXIST) {
            logger.error("Put failed because key {} already exists", keyStr);
        }
    }

    // add val to the keyStr feature if it exists already; otherwise, create the feature
    public void addValFeature(String keyStr, double val, long frequency) {
        double prevVal;

        // key
        DatabaseEntry key = new DatabaseEntry();
        // data
        DatabaseEntry data = new DatabaseEntry();

        StringBinding.stringToEntry(keyStr, key);
        DoubleBinding.doubleToEntry(val, data);

        OperationStatus status = freqDb.get(null, key, data, LockMode.DEFAULT);

        long freq = frequency;
        if (status == OperationStatus.NOTFOUND) {
            status = infreqDb.get(null, key, data, LockMode.DEFAULT);
            if (status == OperationStatus.NOTFOUND) {
                prevVal = 0.0;
            } else {
                prevVal = DoubleBinding.entryToDouble(data);
                freq = FREQUENT_TERMS - 1;
            }
        } else {
            prevVal = DoubleBinding.entryToDouble(data);
            freq = FREQUENT_TERMS + 1;
        }

        putFeature(keyStr, val + prevVal, freq);
    }

    private Database openDb(String path, boolean readOnly) {
        // Open the database. Create it if it does not already exist.
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setReadOnly(readOnly);
        dbConfig.setAllowCreate(!readOnly);
        // Make it deferred write
        if (!readOnly) {
            dbConfig.setDeferredWrite(true);
        }
        // Use a more space-efficient representation
        dbConfig.setKeyPrefixing(true);
        return dbEnv.openDatabase(null, path, dbConfig);
    }

    private void closeDb(Database db) {
        if (db.getConfig().getDeferredWrite()) {
            db.sync();
        }
        db.close();
    }

    private void truncateDb(String path) {
        try {
            long returnCount = dbEnv.truncateDatabase(null, path, true);
            dbEnv.removeDatabase(null, path);
            logger.info("Database {} removed. Discarded {} records.", path, returnCount);

        } catch (DatabaseNotFoundException ex) {
            logger.warn("Database {} not found.", path);
        }
    }

    private void closeEnv(Environment env) {
        if (!env.getConfig().getReadOnly()) {
            env.cleanLog();
        }
        env.close();
    }

}
