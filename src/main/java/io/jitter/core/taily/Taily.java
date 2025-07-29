package io.jitter.core.taily;

import cc.twittertools.index.IndexStatuses;
import io.jitter.core.features.IndriFeature;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

@SuppressWarnings("LoggingSimilarMessage")
public class Taily {
    private static final Logger logger = LoggerFactory.getLogger(Taily.class);

    public static final String CORPUS_DBENV = "corpus";
    public static final String SOURCES_DBENV = "sources";
    public static final String TOPICS_DBENV = "topics";
    public static final int LOG_TERM_INTERVAL = 10000;

    private final String dbPath;
    private final String indexPath;
    private final float mu;

    public Taily(String dbPath, String indexPath, float mu) {
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
    private long collectCorpusStats(TermsEnum termsEnum, FeatureStore store) throws IOException {
        String term = termsEnum.term().utf8ToString();
        double ctf = termsEnum.totalTermFreq();
        double df = termsEnum.docFreq();

        logger.debug(String.format(Locale.ENGLISH, "%s corpus ctf: %4d df: %4d", StringUtils.leftPad(term, 30), (int) ctf, (int) df));

        // store ctf feature for term
        String ctfFeatKey = term + FeatureStore.TERM_SIZE_FEAT_SUFFIX;
        store.addValFeature(ctfFeatKey, ctf, (int) ctf);

        // store df feature for term
        String dfFeatKey = term + FeatureStore.SIZE_FEAT_SUFFIX;
        store.addValFeature(dfFeatKey, df, (int) ctf);

        return (long)ctf;
    }

    public void build(List<String> screenNames, Map<String, List<String>> topics) throws IOException {
        logger.info("build start");
        long startTime = System.currentTimeMillis();

        DirectoryReader indexReader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));

        FeatureStore corpusStore = new RocksDbFeatureStore(dbPath + "/" + CORPUS_DBENV, false);
        buildCorpus(indexReader, corpusStore);

        // get the total term length of the collection (for Indri scoring)
        String totalTermCountKey = FeatureStore.TERM_SIZE_FEAT_SUFFIX;
        double totalTermCount = corpusStore.getFeature(totalTermCountKey);

        IndriFeature indriFeature = new IndriFeature(mu);

        // Reverse map collections -> topic
        Map<String, String> sourceTopicMap = new HashMap<>();
        for (Map.Entry<String, List<String>> entry : topics.entrySet()) {
            for (String collection : entry.getValue()) {
                sourceTopicMap.put(collection.toLowerCase(Locale.ROOT), entry.getKey().toLowerCase(Locale.ROOT));
            }
        }

        int numSources = screenNames.size(); // sourceTopicMap.size();
        Map<String, FeatureStore> sourceStores = new HashMap<>(numSources);
        buildSources(screenNames, indexReader, sourceTopicMap, sourceStores);

        int numTopics = topics.size();
        Map<String, FeatureStore> topicStores = new HashMap<>(numTopics);
        buildTopics(topics, topicStores, sourceStores);

        // TODO: read terms list
        // TODO: field list?
        // TODO: only create shard statistics for specified terms
        Terms terms = MultiTerms.getTerms(indexReader, IndexStatuses.StatusField.TEXT.name);
        TermsEnum termEnum = terms.iterator();

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
            Map<String, ShardData> sourcesShardDataMap = new HashMap<>(numSources);
            Map<String, ShardData> topicsShardDataMap = new HashMap<>(numTopics);

            if (termEnum.seekExact(bytesRef)) {
                PostingsEnum docsEnum = termEnum.postings(null);
                if (docsEnum != null) {
                    int docId;
                    // go through each doc in index containing the current term
                    // calculate Sum(f) and Sum(f^2) top parts of eq (3) (4)
                    while ((docId = docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                        Document doc = indexReader.document(docId);

                        Terms termVector = indexReader.getTermVector(docId, IndexStatuses.StatusField.TEXT.name);
                        double length = termVector.size();
                        double tf = docsEnum.freq();

                        // calculate Indri score feature and sum it up
                        double feat = indriFeature.value(tf, ctf, totalTermCount, length);

                        // find the shard id, if this doc belongs to any
                        String sourceCurrShardId = doc.get(IndexStatuses.StatusField.SCREEN_NAME.name).toLowerCase(Locale.ROOT);
                        updateShardData(sourcesShardDataMap, sourceCurrShardId, feat);

                        // find the shard id, if this doc belongs to any
                        String topicsCurrShardId = sourceTopicMap.get(doc.get(IndexStatuses.StatusField.SCREEN_NAME.name).toLowerCase(Locale.ROOT));
                        updateShardData(topicsShardDataMap, topicsCurrShardId, feat);
                    }
                }
            }

            // add term info to correct shard dbs
            for (String screenName : screenNames) {
                String shardIdStr = screenName.toLowerCase(Locale.ROOT);
                updateStores(sourcesShardDataMap, shardIdStr, sourceStores, term, (int) ctf);
            }

            // add term info to correct shard dbs
            for (String topic : topics.keySet()) {
                String shardIdStr = topic.toLowerCase(Locale.ROOT);
                updateStores(topicsShardDataMap, shardIdStr, topicStores, term, (int) ctf);
            }
        } // end term iter

        // clean up
        corpusStore.close();
        sourceStores.values().forEach(FeatureStore::close);
        topicStores.values().forEach(FeatureStore::close);
        indexReader.close();

        long endTime = System.currentTimeMillis();
        logger.info(String.format(Locale.ENGLISH, "build end %4dms", (endTime - startTime)));
    }

    private void updateStores(Map<String, ShardData> shardDataMap, String shardIdStr, Map<String, FeatureStore> stores, String term, int ctf) {
        // don't store empty terms
        if (shardDataMap.get(shardIdStr) != null) {
            if (shardDataMap.get(shardIdStr).df != 0) {
                logger.debug(String.format(Locale.ENGLISH, "%s shard: %s df: %4d ctf: %4d min: %4.2f f: %4.2f f2: %4.2f",
                        StringUtils.leftPad(term, 30), StringUtils.leftPad(shardIdStr, 15),
                        (long) shardDataMap.get(shardIdStr).df, ctf, shardDataMap.get(shardIdStr).min,
                        shardDataMap.get(shardIdStr).f, shardDataMap.get(shardIdStr).f2));
                storeTermStats(stores.get(shardIdStr), term, ctf, shardDataMap.get(shardIdStr).min,
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

    private void updateShardData(Map<String, ShardData> shardDataMap, String currShardId, double feat) {
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

    private void buildTopics(Map<String, List<String>> topics, Map<String, FeatureStore> topicStores, Map<String, FeatureStore> sourceStores) {
        // create FeatureStore dbs for each topic
        for (String topic : topics.keySet()) {
            String shardIdStr = topic.toLowerCase(Locale.ROOT);
            String cPath = dbPath + "/" + TOPICS_DBENV + "/" + shardIdStr;

            // create feature store for shard
            FeatureStore store = new RocksDbFeatureStore(cPath, false);
            topicStores.put(shardIdStr, store);

            // store the shard size (# of docs) feature
            long totalDocCount = 0;
            for (String collection : topics.get(topic)) {
                totalDocCount += sourceStores.get(collection.toLowerCase(Locale.ROOT)).getFeature(FeatureStore.SIZE_FEAT_SUFFIX);
            }
            store.putFeature(FeatureStore.SIZE_FEAT_SUFFIX, totalDocCount, totalDocCount);

            logger.info("build topics {}: {} = {}", shardIdStr, FeatureStore.SIZE_FEAT_SUFFIX, totalDocCount);
        }
    }

    private void buildSources(List<String> screenNames, DirectoryReader indexReader, Map<String, String> sourceTopicMap, Map<String, FeatureStore> sourceStores) throws IOException {
        Terms screenNameTerms = MultiTerms.getTerms(indexReader, IndexStatuses.StatusField.SCREEN_NAME.name);
        TermsEnum screenNameTermEnum = screenNameTerms.iterator();
        IndexSearcher indexSearcher = new IndexSearcher(indexReader);

        // create FeatureStore dbs for each source
        HashSet<String> allSources = new HashSet<>(screenNames.size());

        for (String screenName : screenNames) {
            allSources.add(screenName.toLowerCase(Locale.ROOT));
        }

        for (String screenName : sourceTopicMap.keySet()) {
            allSources.add(screenName.toLowerCase(Locale.ROOT));
        }

        for (String screenName : allSources) {
            String shardIdStr = screenName;
            String cPath = dbPath + "/" + SOURCES_DBENV + "/" + shardIdStr;

            // create feature store for source
            FeatureStore store = new RocksDbFeatureStore(cPath, false);
            sourceStores.put(shardIdStr, store);

            BytesRef bytesRef = new BytesRef(shardIdStr);
            screenNameTermEnum.seekExact(bytesRef);
            int totalDocCount = screenNameTermEnum.docFreq();

            // fallback for case sensitive indexes
            if (totalDocCount == 0) {
                Term t = new Term(IndexStatuses.StatusField.SCREEN_NAME.name, shardIdStr);
                Query q = new TermQuery(t);

                TotalHitCountCollector totalHitCountCollector = new TotalHitCountCollector();
                indexSearcher.search(q, totalHitCountCollector);
                totalDocCount = totalHitCountCollector.getTotalHits();
            }

            // store the shard size (# of docs) feature
            store.putFeature(FeatureStore.SIZE_FEAT_SUFFIX, totalDocCount, totalDocCount);

            logger.info("build sources {}: {} = {}", shardIdStr, FeatureStore.SIZE_FEAT_SUFFIX, totalDocCount);
        }
    }

    private void buildCorpus(DirectoryReader indexReader, FeatureStore store) throws IOException {
        logger.info("build corpus start");
        long startTime = System.currentTimeMillis();

        // go through all indexes and collect ctf and df statistics.
        long totalTermCount;
        int totalDocCount = 0;

        // TODO: read terms list
        // TODO: field list?
        Terms terms = MultiTerms.getTerms(indexReader, IndexStatuses.StatusField.TEXT.name);
        TermsEnum termEnum = terms.iterator();

        long termCnt = 0;
        BytesRef bytesRef;
        while ((bytesRef = termEnum.next()) != null) {
            String term = bytesRef.utf8ToString();
            if (term.isEmpty()) {
                logger.warn("Empty term was found and skipped automatically. Check your tokenizer.");
                continue;
            }
            termCnt += collectCorpusStats(termEnum, store);
        }

        // add the total term length of shard
        totalTermCount = indexReader.getSumTotalTermFreq(IndexStatuses.StatusField.TEXT.name);
        if (totalTermCount != termCnt) {
            logger.warn("totalTermCount mismatch with loop count");
        }

        // add the shard size (# of docs)
        totalDocCount += indexReader.numDocs();

        // add collection global features needed for shard ranking
        store.putFeature(FeatureStore.SIZE_FEAT_SUFFIX, totalDocCount, FeatureStore.FREQUENT_TERMS + 1);
        store.putFeature(FeatureStore.TERM_SIZE_FEAT_SUFFIX, totalTermCount, FeatureStore.FREQUENT_TERMS + 1);

        logger.info("build corpus {} = {}", FeatureStore.TERM_SIZE_FEAT_SUFFIX, totalTermCount);
        logger.info("build corpus {} = {}", FeatureStore.SIZE_FEAT_SUFFIX, totalDocCount);

        long endTime = System.currentTimeMillis();
        logger.info(String.format(Locale.ENGLISH, "build corpus end %4dms", (endTime - startTime)));
    }

}
