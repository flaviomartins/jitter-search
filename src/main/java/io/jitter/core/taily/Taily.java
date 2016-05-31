package io.jitter.core.taily;

import cc.twittertools.index.IndexStatuses;
import io.jitter.core.features.IndriFeature;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class Taily {
    private static final Logger logger = LoggerFactory.getLogger(Taily.class);

    public static final String CORPUS_DBENV = "corpus";
    public static final String SOURCES_DBENV = "sources";
    public static final String TOPICS_DBENV = "topics";
    public static final int LOG_TERM_INTERVAL = 10000;

    private final String dbPath;
    private final String indexPath;
    private final int mu;

    public Taily(String dbPath, String indexPath, int mu) {
        this.dbPath = dbPath;
        this.indexPath = indexPath;
        this.mu = mu;
    }

    public Taily(String dbPath, String indexPath) {
        this(dbPath, indexPath, IndriFeature.DEFAULT_MU);
    }

    private void storeTermStats(FeatureStore store, String term, int ctf, double min,
                                double df, double f, double f2) {
        // store min feature for term (for this shard; will later be merged into corpus-wide Db)
        String minFeatKey = term + FeatureStore.MIN_FEAT_SUFFIX;
        store.putFeature(minFeatKey, min, ctf);

        // get and store shard df feature for term
        String dfFeatKey = term + FeatureStore.SIZE_FEAT_SUFFIX;
        store.putFeature(dfFeatKey, df, ctf);

        // store sum f
        String featKey = term + FeatureStore.FEAT_SUFFIX;
        store.putFeature(featKey, f, ctf);

        // store sum f^2
        String squaredFeatKey = term + FeatureStore.SQUARED_FEAT_SUFFIX;
        store.putFeature(squaredFeatKey, f2, ctf);
    }

    // innards of buildCorpus
    private void collectCorpusStats(TermsEnum termsEnum, FeatureStore store) throws IOException {
        String term = termsEnum.term().utf8ToString();
        double ctf = termsEnum.totalTermFreq();
        double df = termsEnum.docFreq();

        logger.debug(String.format(Locale.ENGLISH, "term: %s ctf: %d df: %d", term, (int) ctf, (int) df));

        // store ctf feature for term
        String ctfFeatKey = term + FeatureStore.TERM_SIZE_FEAT_SUFFIX;
        store.addValFeature(ctfFeatKey, ctf, (int) ctf);

        // store df feature for term
        String dfFeatKey = term + FeatureStore.SIZE_FEAT_SUFFIX;
        store.addValFeature(dfFeatKey, df, (int) ctf);
    }

    public void buildCorpus() throws IOException {
        logger.info("build corpus start");

        DirectoryReader indexReader = DirectoryReader.open(FSDirectory.open(new File(indexPath)));
        FeatureStore store = new FeatureStore(dbPath + "/" + CORPUS_DBENV, false);

        // go through all indexes and collect ctf and df statistics.
        int totalTermCount = 0;
        int totalDocCount = 0;

        // TODO: read terms list
        // TODO: field list?
        Terms terms = MultiFields.getTerms(indexReader, IndexStatuses.StatusField.TEXT.name);
        TermsEnum termEnum = terms.iterator(null);

        int termCnt = 0;
        BytesRef bytesRef;
        while ((bytesRef = termEnum.next()) != null) {
            String term = bytesRef.utf8ToString();
            if (term.isEmpty()) {
                logger.warn("Empty term was found and skipped automatically. Check your tokenizer.");
                continue;
            }
            collectCorpusStats(termEnum, store);
            termCnt++;
        }

        // add the total term length of shard
        totalTermCount += termCnt;
        // add the shard size (# of docs)
        totalDocCount += indexReader.numDocs();

        // add collection global features needed for shard ranking
        store.putFeature(FeatureStore.SIZE_FEAT_SUFFIX, totalDocCount, FeatureStore.FREQUENT_TERMS + 1);
        store.putFeature(FeatureStore.TERM_SIZE_FEAT_SUFFIX, totalTermCount, FeatureStore.FREQUENT_TERMS + 1);

        logger.info("build corpus {} = {}", FeatureStore.TERM_SIZE_FEAT_SUFFIX, totalTermCount);
        logger.info("build corpus {} = {}", FeatureStore.SIZE_FEAT_SUFFIX, totalDocCount);

        store.close();
        indexReader.close();

        logger.info("build corpus end");
    }

    public void buildFromSources(List<String> screenNames) throws IOException {
        logger.info("build sources start");

        DirectoryReader indexReader = DirectoryReader.open(FSDirectory.open(new File(indexPath)));
        IndexSearcher indexSearcher = new IndexSearcher(indexReader);

        FeatureStore corpusStore = new FeatureStore(dbPath + "/" + CORPUS_DBENV, false);
        Map<String, FeatureStore> stores = new HashMap<>();

        // create FeatureStore dbs for each shard
        for (String screenName : screenNames) {
            String shardIdStr = screenName.toLowerCase(Locale.ROOT);
            String cPath = dbPath + "/" + SOURCES_DBENV + "/" + shardIdStr;

            // create feature store for shard
            FeatureStore store = new FeatureStore(cPath, false);
            stores.put(shardIdStr, store);

            Term t = new Term(IndexStatuses.StatusField.SCREEN_NAME.name, shardIdStr);
            Query q = new TermQuery(t);

            TotalHitCountCollector totalHitCountCollector = new TotalHitCountCollector();
            indexSearcher.search(q, totalHitCountCollector);
            int totalDocCount = totalHitCountCollector.getTotalHits();

            // store the shard size (# of docs) feature
            store.putFeature(FeatureStore.SIZE_FEAT_SUFFIX, totalDocCount, totalDocCount);

            logger.info("build sources {}: {} = {}", shardIdStr, FeatureStore.SIZE_FEAT_SUFFIX, totalDocCount);
        }

        // get the total term length of the collection (for Indri scoring)
        String totalTermCountKey = FeatureStore.TERM_SIZE_FEAT_SUFFIX;
        double totalTermCount = corpusStore.getFeature(totalTermCountKey);

        IndriFeature indriFeature = new IndriFeature(mu);

        Bits liveDocs = MultiFields.getLiveDocs(indexReader);
        // TODO: read terms list
        // TODO: field list?
        // TODO: only create shard statistics for specified terms
        Terms terms = MultiFields.getTerms(indexReader, IndexStatuses.StatusField.TEXT.name);
        TermsEnum termEnum = terms.iterator(null);

        int termCnt = 0;
        BytesRef bytesRef;
        while ((bytesRef = termEnum.next()) != null) {
            String term = bytesRef.utf8ToString();
            if (term.isEmpty()) {
                logger.warn("Empty term was found and skipped automatically. Check your tokenizer.");
                continue;
            }

            termCnt++;
            if (termCnt % LOG_TERM_INTERVAL == 0) {
                logger.info("  Finished {} terms", termCnt);
            }

            // get term ctf
            double ctf = termEnum.totalTermFreq();

            // track df for this term for each shard; initialize
            Map<String, ShardData> shardDataMap = new HashMap<>();

            if (termEnum.seekExact(bytesRef)) {

                DocsEnum docsEnum = termEnum.docs(liveDocs, null);

                if (docsEnum != null) {
                    int docId;
                    // go through each doc in index containing the current term
                    // calculate Sum(f) and Sum(f^2) top parts of eq (3) (4)
                    while ((docId = docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                        Document doc = indexReader.document(docId);

                        // find the shard id, if this doc belongs to any
                        String currShardId = doc.get(IndexStatuses.StatusField.SCREEN_NAME.name).toLowerCase(Locale.ROOT);

                        double length = doc.getValues(IndexStatuses.StatusField.TEXT.name).length;
                        double tf = docsEnum.freq();

                        // calculate Indri score feature and sum it up
                        double feat = indriFeature.value(tf, ctf, totalTermCount, length);

                        ShardData currShard = shardDataMap.get(currShardId);
                        if (currShard == null) {
                            currShard = new ShardData();
                            shardDataMap.put(currShardId, currShard);
                        }
                        currShard.f += feat;
                        currShard.f2 += Math.pow(feat, 2);
                        currShard.df += 1;

                        if (feat < currShard.min) {
                            currShard.min = feat;
                        }
                    }
                }
            }

            // add term info to correct shard dbs
            for (String screenName : screenNames) {
                String shardIdStr = screenName.toLowerCase(Locale.ROOT);
                // don't store empty terms
                if (shardDataMap.get(shardIdStr) != null) {
                    if (shardDataMap.get(shardIdStr).df != 0) {
                        logger.debug(String.format(Locale.ENGLISH, "shard: %s term: %s ctf: %d min: %.2f shardDf: %d f: %.2f f2: %.2f", shardIdStr, term, (int) ctf, shardDataMap.get(shardIdStr).min,
                                (long) shardDataMap.get(shardIdStr).df, shardDataMap.get(shardIdStr).f,
                                shardDataMap.get(shardIdStr).f2));
                        storeTermStats(stores.get(shardIdStr), term, (int) ctf, shardDataMap.get(shardIdStr).min,
                                shardDataMap.get(shardIdStr).df, shardDataMap.get(shardIdStr).f,
                                shardDataMap.get(shardIdStr).f2);

                        // store min feature for term (for corpus)
//                        String minFeatKey = term + FeatureStore.MIN_FEAT_SUFFIX;
//                        double min = corpusStore.getFeature(minFeatKey);
//                        if (min > shardDataMap.get(shardIdStr).min) {
//                            corpusStore.putFeature(minFeatKey, shardDataMap.get(shardIdStr).min, FeatureStore.FREQUENT_TERMS + 1);
//                        }
                    }
                }
            }
        } // end term iter

        // clean up
        corpusStore.close();

        stores.values().forEach(FeatureStore::close);

        indexReader.close();

        logger.info("build sources end");
    }

    public void buildFromTopics(Map<String, List<String>> topics) throws IOException {
        logger.info("build topics start");

        // Reverse map collections -> topic
        Map<String, String> sourceTopicMap = new HashMap<>();
        for (Map.Entry<String, List<String>> entry : topics.entrySet()) {
            for (String collection : entry.getValue()) {
                sourceTopicMap.put(collection.toLowerCase(Locale.ROOT), entry.getKey().toLowerCase(Locale.ROOT));
            }
        }

        DirectoryReader indexReader = DirectoryReader.open(FSDirectory.open(new File(indexPath)));

        FeatureStore corpusStore = new FeatureStore(dbPath + "/" + CORPUS_DBENV, false);
        Map<String, FeatureStore> sourceStores = new HashMap<>();
        Map<String, FeatureStore> stores = new HashMap<>();

        // open FeatureStore dbs for each source
        for (String shardIdStr : sourceTopicMap.keySet()) {
            String cPath = dbPath + "/" + SOURCES_DBENV + "/" + shardIdStr;

            // create feature store for shard
            FeatureStore store = new FeatureStore(cPath, true);
            sourceStores.put(shardIdStr, store);
        }

        // create FeatureStore dbs for each shard
        for (String topic : topics.keySet()) {
            String shardIdStr = topic.toLowerCase(Locale.ROOT);
            String cPath = dbPath + "/" + TOPICS_DBENV + "/" + shardIdStr;

            // create feature store for shard
            FeatureStore store = new FeatureStore(cPath, false);
            stores.put(shardIdStr, store);

            // store the shard size (# of docs) feature
            double totalDocCount = 0;
            for (String collection : topics.get(topic)) {
                totalDocCount += sourceStores.get(collection.toLowerCase(Locale.ROOT)).getFeature(FeatureStore.SIZE_FEAT_SUFFIX);
            }
            store.putFeature(FeatureStore.SIZE_FEAT_SUFFIX, totalDocCount, (int) totalDocCount);

            logger.info("build topics {}: {} = {}", shardIdStr, FeatureStore.SIZE_FEAT_SUFFIX, (int) totalDocCount);
        }

        // get the total term length of the collection (for Indri scoring)
        String totalTermCountKey = FeatureStore.TERM_SIZE_FEAT_SUFFIX;
        double totalTermCount = corpusStore.getFeature(totalTermCountKey);

        IndriFeature indriFeature = new IndriFeature(mu);

        Bits liveDocs = MultiFields.getLiveDocs(indexReader);
        // TODO: read terms list
        // TODO: field list?
        // TODO: only create shard statistics for specified terms
        Terms terms = MultiFields.getTerms(indexReader, IndexStatuses.StatusField.TEXT.name);
        TermsEnum termEnum = terms.iterator(null);

        int termCnt = 0;
        BytesRef bytesRef;
        while ((bytesRef = termEnum.next()) != null) {
            String term = bytesRef.utf8ToString();
            if (term.isEmpty()) {
                logger.warn("Empty term was found and skipped automatically. Check your tokenizer.");
                continue;
            }

            termCnt++;
            if (termCnt % LOG_TERM_INTERVAL == 0) {
                logger.info("  Finished " + termCnt + " terms");
            }

            // get term ctf
            double ctf = termEnum.totalTermFreq();

            // track df for this term for each shard; initialize
            Map<String, ShardData> shardDataMap = new HashMap<>();

            if (termEnum.seekExact(bytesRef)) {

                DocsEnum docsEnum = termEnum.docs(liveDocs, null);

                if (docsEnum != null) {
                    int docId;
                    // go through each doc in index containing the current term
                    // calculate Sum(f) and Sum(f^2) top parts of eq (3) (4)
                    while ((docId = docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                        Document doc = indexReader.document(docId);

                        // find the shard id, if this doc belongs to any
                        String currShardId = sourceTopicMap.get(doc.get(IndexStatuses.StatusField.SCREEN_NAME.name).toLowerCase(Locale.ROOT));

                        double length = doc.getValues(IndexStatuses.StatusField.TEXT.name).length;
                        double tf = docsEnum.freq();

                        // calculate Indri score feature and sum it up
                        double feat = indriFeature.value(tf, ctf, totalTermCount, length);

                        ShardData currShard = shardDataMap.get(currShardId);
                        if (currShard == null) {
                            currShard = new ShardData();
                            shardDataMap.put(currShardId, currShard);
                        }
                        currShard.f += feat;
                        currShard.f2 += Math.pow(feat, 2);
                        currShard.df += 1;

                        if (feat < currShard.min) {
                            currShard.min = feat;
                        }
                    }
                }
            }

            // add term info to correct shard dbs
            for (String topic : topics.keySet()) {
                String shardIdStr = topic.toLowerCase(Locale.ROOT);
                // don't store empty terms
                if (shardDataMap.get(shardIdStr) != null) {
                    if (shardDataMap.get(shardIdStr).df != 0) {
                        logger.debug(String.format(Locale.ENGLISH, "shard: %s term: %s ctf: %d min: %.2f shardDf: %d f: %.2f f2: %.2f", shardIdStr, term, (int) ctf, shardDataMap.get(shardIdStr).min,
                                (long) shardDataMap.get(shardIdStr).df, shardDataMap.get(shardIdStr).f,
                                shardDataMap.get(shardIdStr).f2));
                        storeTermStats(stores.get(shardIdStr), term, (int) ctf, shardDataMap.get(shardIdStr).min,
                                shardDataMap.get(shardIdStr).df, shardDataMap.get(shardIdStr).f,
                                shardDataMap.get(shardIdStr).f2);

                        // store min feature for term (for corpus)
//                        String minFeatKey = term + FeatureStore.MIN_FEAT_SUFFIX;
//                        double min = corpusStore.getFeature(minFeatKey);
//                        if (min > shardDataMap.get(shardIdStr).min) {
//                            corpusStore.putFeature(minFeatKey, shardDataMap.get(shardIdStr).min, FeatureStore.FREQUENT_TERMS + 1);
//                        }
                    }
                }
            }
        } // end term iter

        // clean up
        corpusStore.close();

        sourceStores.values().forEach(FeatureStore::close);

        stores.values().forEach(FeatureStore::close);

        indexReader.close();

        logger.info("build topics end");
    }

}
