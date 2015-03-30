package org.novasearch.jitter.core.selection.taily;

import cc.twittertools.index.IndexStatuses;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Taily {
    private static final Logger logger = Logger.getLogger(Taily.class);

    private static final int DEFAULT_MU = 2500;

    private final String indexPath;
    private final int mu;

    public Taily(String indexPath, int mu) {
        this.indexPath = indexPath;
        this.mu = mu;
    }

    public Taily(String indexPath) {
        this(indexPath, DEFAULT_MU);
    }

    private double calcIndriFeature(double tf, double ctf, double totalTermCount, double docLength, int mu) {
        return Math.log((tf + mu * (ctf / totalTermCount)) / (docLength + mu));
    }

    class ShardData {
        double min;
        double shardDf;
        double f;
        double f2;

        public ShardData() {
            min = Double.MAX_VALUE;
            shardDf = 0;
            f = 0;
            f2 = 0;
        }

        public ShardData(double min, double shardDf, double f, double f2) {
            this.min = min;
            this.shardDf = shardDf;
            this.f = f;
            this.f2 = f2;
        }
    }

    public void storeTermStats(FeatureStore store, String term, int ctf, double min,
                        double shardDf, double f, double f2) {
        // store min feature for term (for this shard; will later be merged into corpus-wide Db)
        String minFeatKey = term + FeatureStore.MIN_FEAT_SUFFIX;
        store.putFeature(minFeatKey, min, ctf);

        // get and store shard df feature for term
        String dfFeatKey = term + FeatureStore.SIZE_FEAT_SUFFIX;
        store.putFeature(dfFeatKey, shardDf, ctf);

        // store sum f
        String featKey = term + FeatureStore.FEAT_SUFFIX;
        store.putFeature(featKey, f, ctf);

        // store sum f^2
        String squaredFeatKey = term + FeatureStore.SQUARED_FEAT_SUFFIX;
        store.putFeature(squaredFeatKey, f2, ctf);
    }

    public void buildCorpus(String dbPath) throws IOException {
        // TODO: read terms list

        FeatureStore store = new FeatureStore(dbPath, false);

        DirectoryReader indexReader = DirectoryReader.open(FSDirectory.open(new File(indexPath)));

        // go through all indexes and collect ctf and df statistics.
        long totalTermCount = 0;
        long totalDocCount = 0;

        logger.info("Starting index");

        // TODO: field list?
        Terms terms = MultiFields.getTerms(indexReader, IndexStatuses.StatusField.TEXT.name);
        TermsEnum termEnum = terms.iterator(null);

        int termCnt = 0;
        BytesRef bytesRef;
        while ((bytesRef = termEnum.next()) != null) {
            collectCorpusStats(termEnum, store);
            termCnt++;
        }

        // add the total term length of shard
        totalTermCount += termCnt;
        // add the shard size (# of docs)
        totalDocCount += indexReader.numDocs();

        // add collection global features needed for shard ranking
        store.putFeature(FeatureStore.TERM_SIZE_FEAT_SUFFIX, totalTermCount, FeatureStore.FREQUENT_TERMS+1);
        logger.debug(FeatureStore.TERM_SIZE_FEAT_SUFFIX + " " + totalTermCount);
        store.putFeature(FeatureStore.SIZE_FEAT_SUFFIX, totalDocCount, FeatureStore.FREQUENT_TERMS + 1);
        logger.debug(FeatureStore.SIZE_FEAT_SUFFIX + " " + totalDocCount);

        store.close();
        indexReader.close();
    }

    // innards of buildcorpus
    private void collectCorpusStats(TermsEnum termEnum, FeatureStore store) throws IOException {
        String term = termEnum.term().utf8ToString();
        double ctf = termEnum.totalTermFreq();
        double df = termEnum.docFreq();

        logger.debug(String.format("term: %s ctf: %d df: %d", term, (int) ctf, (int) df));

        // store ctf feature for term
        String ctfFeatKey = term + FeatureStore.TERM_SIZE_FEAT_SUFFIX;
        store.addValFeature(ctfFeatKey, ctf, (int) ctf);

        // store df feature for term
        String dfFeatKey = term + FeatureStore.SIZE_FEAT_SUFFIX;
        store.addValFeature(dfFeatKey, df, (int) ctf);
    }

    public void buildFromMap(String dbPath, List<String> screenNames) throws IOException {
        // TODO: read terms list

        DirectoryReader indexReader = DirectoryReader.open(FSDirectory.open(new File(indexPath)));

        // open up all output feature storages for each mapping file we are accessing
        Map<String, FeatureStore> stores = new HashMap<>();

        // read in the mapping files given and construct a reverse mapping,
        // i.e. doc -> shard, and create FeatureStore dbs for each shard
        for(String shardIdStr: screenNames) {
            String cPath = dbPath+"/"+shardIdStr;

            // create feature store for shard
            FeatureStore store = new FeatureStore(cPath, false);
            stores.put(shardIdStr, store);

            // TODO: store the shard size (# of docs) feature
//            store.putFeature(FeatureStore.SIZE_FEAT_SUFFIX, (double) totalDocCount, totalDocCount);
        }

        logger.info("Finished reading shard map.");

        // get the total term length of the collection (for Indri scoring)
        double totalTermCount = 0;
        // TODO: field list?
        Terms terms = MultiFields.getTerms(indexReader, IndexStatuses.StatusField.TEXT.name);
        TermsEnum termEnum = terms.iterator(null);

        int termCnt = 0;
        BytesRef bytesRef;
        while ((bytesRef = termEnum.next()) != null) {
            termCnt++;
        }

        // add the total term length of shard
        totalTermCount += termCnt;

        // TODO: only create shard statistics for specified terms

        Bits liveDocs = MultiFields.getLiveDocs(indexReader);
        termEnum = terms.iterator(null);

        termCnt = 0;
        while ((bytesRef = termEnum.next()) != null) {
            termCnt++;
            if (termCnt % 1000 == 0) {
                logger.info("  Finished " + termCnt + " terms");
            }

            String term = bytesRef.utf8ToString();

            // get term ctf
            double ctf = 0;
//    string ctfKey(stem);
//    ctfKey.append(FeatureStore::TERM_SIZE_FEAT_SUFFIX);
//    corpusStats.getFeature((char*) ctfKey.c_str(), &ctf);

            ctf += termEnum.totalTermFreq();

            //track df for this term for each shard; initialize
            Map<String, ShardData> shardDataMap = new HashMap<>();

            if (termEnum.seekExact(bytesRef, true)) {

                DocsEnum docsEnum = termEnum.docs(liveDocs, null);

                if (docsEnum != null) {
                    int docId;
                    // go through each doc in index containing the current term
                    // calculate Sum(f) and Sum(f^2) top parts of eq (3) (4)
                    while ((docId = docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                        Document doc = indexReader.document(docId);

                        // find the shard id, if this doc belongs to any
                        String currShardId = doc.get(IndexStatuses.StatusField.SCREEN_NAME.name).toLowerCase();

                        double length = doc.getValues(IndexStatuses.StatusField.TEXT.name).length;
                        double tf = docsEnum.freq();

                        // calulate Indri score feature and sum it up
                        double feat = calcIndriFeature(tf, ctf, totalTermCount, length, mu);

                        ShardData currShard = shardDataMap.get(currShardId);
                        if (currShard == null) {
                            currShard = new ShardData();
                            shardDataMap.put(currShardId, currShard);
                        }
                        currShard.f += feat;
                        currShard.f2 += Math.pow(feat, 2);
                        currShard.shardDf += 1;

                        if (feat < currShard.min) {
                            currShard.min = feat;
                        }
                    }
                }
            }

            // add term info to correct shard dbs
            for(String screenName: screenNames) {
                String shardIdStr = screenName.toLowerCase();
                // don't store empty terms
                if (shardDataMap.get(shardIdStr) != null) {
                    if (shardDataMap.get(shardIdStr).shardDf != 0) {
                        logger.debug(String.format("shard: %s term: %s ctf: %d min: %.2f shardDf: %d f: %.2f f2: %.2f", shardIdStr, term, (int) ctf, shardDataMap.get(shardIdStr).min,
                                (long) shardDataMap.get(shardIdStr).shardDf, shardDataMap.get(shardIdStr).f,
                                shardDataMap.get(shardIdStr).f2));
                        storeTermStats(stores.get(shardIdStr), term, (int) ctf, shardDataMap.get(shardIdStr).min,
                                shardDataMap.get(shardIdStr).shardDf, shardDataMap.get(shardIdStr).f,
                                shardDataMap.get(shardIdStr).f2);
                    }
                }
            }

            // add the total term length of the collection
            totalTermCount += termCnt;
//        totalTermCount = corpusStats.getFeature(FeatureStore.TERM_SIZE_FEAT_SUFFIX);
        } // end term iter

        // clean up
        for (FeatureStore store : stores.values()) {
            store.close();
        }

        indexReader.close();
    }

    public void buildFromMapTopics(String dbPath, Map<String, List<String>> topics) throws IOException {
        Map<String, String> sourceTopicMap = new HashMap<>();
        for (Map.Entry<String, List<String>> entry : topics.entrySet()) {
            for (String source : entry.getValue()) {
                sourceTopicMap.put(source.toLowerCase(), entry.getKey());
            }
        }

        // TODO: read terms list

        DirectoryReader indexReader = DirectoryReader.open(FSDirectory.open(new File(indexPath)));

        // open up all output feature storages for each mapping file we are accessing
        Map<String, FeatureStore> stores = new HashMap<>();

        // read in the mapping files given and construct a reverse mapping,
        // i.e. doc -> shard, and create FeatureStore dbs for each shard
        for(String topic: topics.keySet()) {
            String shardIdStr = topic.toLowerCase();
            String cPath = dbPath+"/"+shardIdStr;

            // create feature store for shard
            FeatureStore store = new FeatureStore(cPath, false);
            stores.put(shardIdStr, store);

            // TODO: store the shard size (# of docs) feature
//            store.putFeature(FeatureStore.SIZE_FEAT_SUFFIX, (double) totalDocCount, totalDocCount);
        }

        logger.info("Finished reading shard map.");

        // get the total term length of the collection (for Indri scoring)
        double totalTermCount = 0;
        // TODO: field list?
        Terms terms = MultiFields.getTerms(indexReader, IndexStatuses.StatusField.TEXT.name);
        TermsEnum termEnum = terms.iterator(null);

        int termCnt = 0;
        BytesRef bytesRef;
        while ((bytesRef = termEnum.next()) != null) {
            termCnt++;
        }

        // add the total term length of shard
        totalTermCount += termCnt;

        // TODO: only create shard statistics for specified terms

        Bits liveDocs = MultiFields.getLiveDocs(indexReader);
        termEnum = terms.iterator(null);

        termCnt = 0;
        while ((bytesRef = termEnum.next()) != null) {
            termCnt++;
            if (termCnt % 1000 == 0) {
                logger.info("  Finished " + termCnt + " terms");
            }

            String term = bytesRef.utf8ToString();

            // get term ctf
            double ctf = 0;
//    string ctfKey(stem);
//    ctfKey.append(FeatureStore::TERM_SIZE_FEAT_SUFFIX);
//    corpusStats.getFeature((char*) ctfKey.c_str(), &ctf);

            ctf += termEnum.totalTermFreq();

            //track df for this term for each shard; initialize
            Map<String, ShardData> shardDataMap = new HashMap<>();

            if (termEnum.seekExact(bytesRef, true)) {

                DocsEnum docsEnum = termEnum.docs(liveDocs, null);

                if (docsEnum != null) {
                    int docId;
                    // go through each doc in index containing the current term
                    // calculate Sum(f) and Sum(f^2) top parts of eq (3) (4)
                    while ((docId = docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                        Document doc = indexReader.document(docId);

                        // find the shard id, if this doc belongs to any
                        String currShardId = sourceTopicMap.get(doc.get(IndexStatuses.StatusField.SCREEN_NAME.name).toLowerCase());

                        double length = doc.getValues(IndexStatuses.StatusField.TEXT.name).length;
                        double tf = docsEnum.freq();

                        // calulate Indri score feature and sum it up
                        double feat = calcIndriFeature(tf, ctf, totalTermCount, length, mu);

                        ShardData currShard = shardDataMap.get(currShardId);
                        if (currShard == null) {
                            currShard = new ShardData();
                            shardDataMap.put(currShardId, currShard);
                        }
                        currShard.f += feat;
                        currShard.f2 += Math.pow(feat, 2);
                        currShard.shardDf += 1;

                        if (feat < currShard.min) {
                            currShard.min = feat;
                        }
                    }
                }
            }

            // add term info to correct shard dbs
            for(String topic: topics.keySet()) {
                String shardIdStr = topic.toLowerCase();
                // don't store empty terms
                if (shardDataMap.get(shardIdStr) != null) {
                    if (shardDataMap.get(shardIdStr).shardDf != 0) {
                        logger.debug(String.format("shard: %s term: %s ctf: %d min: %.2f shardDf: %d f: %.2f f2: %.2f", shardIdStr, term, (int) ctf, shardDataMap.get(shardIdStr).min,
                                (long) shardDataMap.get(shardIdStr).shardDf, shardDataMap.get(shardIdStr).f,
                                shardDataMap.get(shardIdStr).f2));
                        storeTermStats(stores.get(shardIdStr), term, (int) ctf, shardDataMap.get(shardIdStr).min,
                                shardDataMap.get(shardIdStr).shardDf, shardDataMap.get(shardIdStr).f,
                                shardDataMap.get(shardIdStr).f2);
                    }
                }
            }

            // add the total term length of the collection
            totalTermCount += termCnt;
//        totalTermCount = corpusStats.getFeature(FeatureStore.TERM_SIZE_FEAT_SUFFIX);
        } // end term iter

        // clean up
        for (FeatureStore store : stores.values()) {
            store.close();
        }

        indexReader.close();
    }

}
