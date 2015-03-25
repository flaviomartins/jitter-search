package org.novasearch.jitter.core.selection.taily;

import com.sleepycat.bind.tuple.DoubleBinding;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.*;
import org.apache.log4j.Logger;

import java.io.File;

public class FeatureStore {
    private static final Logger logger = Logger.getLogger(FeatureStore.class);

    public static final String FEAT_SUFFIX = "#f";
    public static final String SQUARED_FEAT_SUFFIX = "#f2";
    public static final String MIN_FEAT_SUFFIX = "#m";
    public static final String SIZE_FEAT_SUFFIX = "#d";
    public static final String TERM_SIZE_FEAT_SUFFIX = "#t";
    public static final int FREQUENT_TERMS = 1000; // tf required for a term to be considered "frequent"
    public static final int MAX_TERM_SIZE = 512;

    private Environment dbEnv;
    private Database freqDb;
    private Database infreqDb;

    public FeatureStore(String dir, boolean readOnly) {
        String freqPath = dir + "/freq.db";
        String infreqPath = dir + "/infreq.db";

        try {
            // Instantiate an environment configuration object
            EnvironmentConfig envConfig = new EnvironmentConfig();
            // Configure the environment for the read-only state as identified
            // by the readOnly parameter on this method call.
            envConfig.setReadOnly(readOnly);
            // If the environment is opened for write, then we want to be
            // able to create the environment if it does not exist.
            envConfig.setAllowCreate(!readOnly);
            // Instantiate the Environment. This opens it and also possibly
            // creates it.
            dbEnv = new Environment(new File(dir), envConfig);
        } catch (DatabaseException dbe) {
            logger.error(dbe.getMessage());
        }

        freqDb = openDb(freqPath, readOnly);
        infreqDb = openDb(infreqPath, readOnly);
    }

    public void close() {
        closeDb(freqDb);
        closeDb(infreqDb);
    }

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

        double val = DoubleBinding.entryToDouble(data);

        return val;
    }

    public void putFeature(String keyStr, double val, int frequency) {
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
            logger.error(String.format("Put failed because key %s already exists", keyStr));
        }
    }

    public void addValFeature(String keyStr, double val, int frequency) {
        double prevVal;

        // key
        DatabaseEntry key = new DatabaseEntry();
        // data
        DatabaseEntry data = new DatabaseEntry();

        StringBinding.stringToEntry(keyStr, key);
        DoubleBinding.doubleToEntry(val, data);

        OperationStatus status = freqDb.get(null, key, data, LockMode.DEFAULT);

        if (status == OperationStatus.NOTFOUND) {
            status = infreqDb.get(null, key, data, LockMode.DEFAULT);
            if (status == OperationStatus.NOTFOUND) {
                prevVal = 0.0;
            } else {
                prevVal = DoubleBinding.entryToDouble(data);
                frequency = FREQUENT_TERMS - 1;
            }
        } else {
            prevVal = DoubleBinding.entryToDouble(data);
            frequency = FREQUENT_TERMS + 1;
        }

        putFeature(keyStr, val+prevVal, frequency);
    }

    private Database openDb(String path, boolean readOnly) {
        // Open the database. Create it if it does not already exist.
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(!readOnly);
        // Make it deferred write
        dbConfig.setDeferredWrite(true);
        return dbEnv.openDatabase(null, path, dbConfig);
    }

    private void closeDb(Database db) {
        db.close();
    }

    public static void main(String[] args) {
        FeatureStore store = new FeatureStore("bdb", false);
        System.out.println(store.getFeature("wow"));
        store.putFeature("wow", 1, 100);
        store.addValFeature("wow", 10, 100);
        System.out.println(store.getFeature("wow"));
        store.close();
    }

}
