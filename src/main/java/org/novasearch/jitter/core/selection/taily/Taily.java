package org.novasearch.jitter.core.selection.taily;

import cc.twittertools.index.IndexStatuses;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
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

    public static final String CORPUS_DBENV = "corpus";
    public static final String SOURCES_DBENV = "sources";
    public static final String TOPICS_DBENV = "topics";

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

    public void storeTermStats(FeatureStore store, String term, int ctf, double min,
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

    // innards of buildcorpus
    private void collectCorpusStats(TermsEnum termsEnum, FeatureStore store) throws IOException {
        String term = termsEnum.term().utf8ToString();
        double ctf = termsEnum.totalTermFreq();
        double df = termsEnum.docFreq();

        logger.debug(String.format("term: %s ctf: %d df: %d", term, (int) ctf, (int) df));

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
        long totalTermCount = 0;
        long totalDocCount = 0;

        // TODO: read terms list
        // TODO: field list?
        Terms terms = MultiFields.getTerms(indexReader, IndexStatuses.StatusField.TEXT.name);
        TermsEnum termEnum = terms.iterator(null);

        int termCnt = 0;
        BytesRef bytesRef;
        while ((bytesRef = termEnum.next()) != null) {
            String term = bytesRef.utf8ToString();
            if (term.isEmpty()) {
                continue;
            }
            collectCorpusStats(termEnum, store);
            termCnt++;
        }

        // add the total term length of shard
        totalTermCount += termCnt;
        // add the shard size (# of docs)
        totalDocCount += indexReader.numDocs();

        logger.debug(String.format("build corpus %s = %d", FeatureStore.TERM_SIZE_FEAT_SUFFIX, totalTermCount));
        logger.debug(String.format("build corpus %s = %d", FeatureStore.SIZE_FEAT_SUFFIX, totalDocCount));

        // add collection global features needed for shard ranking
        store.putFeature(FeatureStore.TERM_SIZE_FEAT_SUFFIX, totalTermCount, FeatureStore.FREQUENT_TERMS + 1);
        store.putFeature(FeatureStore.SIZE_FEAT_SUFFIX, totalDocCount, FeatureStore.FREQUENT_TERMS + 1);

        store.close();
        indexReader.close();

        logger.info("build corpus end");
    }

    public void buildFromSources(List<String> screenNames) throws IOException {
        logger.info("build sources start");

        DirectoryReader indexReader = DirectoryReader.open(FSDirectory.open(new File(indexPath)));
        IndexSearcher indexSearcher = new IndexSearcher(indexReader);

        FeatureStore corpusStore = new FeatureStore(dbPath + "/" + CORPUS_DBENV, true);
        Map<String, FeatureStore> stores = new HashMap<>();

        // read in the mapping files given and construct a reverse mapping,
        // i.e. doc -> shard, and create FeatureStore dbs for each shard
        for (String shardIdStr : screenNames) {
            String cPath = dbPath + "/" + SOURCES_DBENV + "/" + shardIdStr;

            // create feature store for shard
            FeatureStore store = new FeatureStore(cPath, false);
            stores.put(shardIdStr, store);

            // store the shard size (# of docs) feature
            Query q = new TermQuery(new Term(IndexStatuses.StatusField.SCREEN_NAME.name, shardIdStr));
            TopDocs topDocs = indexSearcher.search(q, 10);
            double totalDocCount = topDocs.totalHits;
            store.putFeature(FeatureStore.SIZE_FEAT_SUFFIX, totalDocCount, (int) totalDocCount);
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
            termCnt++;
            if (termCnt % 1000 == 0) {
                logger.info("  Finished " + termCnt + " terms");
            }

            String term = bytesRef.utf8ToString();
            if (term.isEmpty()) {
                continue;
            }

            // get term ctf
            double ctf = termEnum.totalTermFreq();

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
            for(String screenName: screenNames) {
                String shardIdStr = screenName.toLowerCase();
                // don't store empty terms
                if (shardDataMap.get(shardIdStr) != null) {
                    if (shardDataMap.get(shardIdStr).df != 0) {
                        logger.debug(String.format("shard: %s term: %s ctf: %d min: %.2f shardDf: %d f: %.2f f2: %.2f", shardIdStr, term, (int) ctf, shardDataMap.get(shardIdStr).min,
                                (long) shardDataMap.get(shardIdStr).df, shardDataMap.get(shardIdStr).f,
                                shardDataMap.get(shardIdStr).f2));
                        storeTermStats(stores.get(shardIdStr), term, (int) ctf, shardDataMap.get(shardIdStr).min,
                                shardDataMap.get(shardIdStr).df, shardDataMap.get(shardIdStr).f,
                                shardDataMap.get(shardIdStr).f2);
                    }
                }
            }
        } // end term iter

        // clean up
        corpusStore.close();

        for (FeatureStore store : stores.values()) {
            store.close();
        }

        indexReader.close();

        logger.info("build sources end");
    }

    public void buildFromTopics(Map<String, List<String>> topics) throws IOException {
        logger.info("build topics start");

        // Reverse map sources -> topic
        Map<String, String> sourceTopicMap = new HashMap<>();
        for (Map.Entry<String, List<String>> entry : topics.entrySet()) {
            for (String source : entry.getValue()) {
                sourceTopicMap.put(source.toLowerCase(), entry.getKey());
            }
        }

        DirectoryReader indexReader = DirectoryReader.open(FSDirectory.open(new File(indexPath)));
        IndexSearcher indexSearcher = new IndexSearcher(indexReader);

        FeatureStore corpusStore = new FeatureStore(dbPath + "/" + CORPUS_DBENV, true);
        Map<String, FeatureStore> sourcesStores = new HashMap<>();
        Map<String, FeatureStore> stores = new HashMap<>();

        // read in the mapping files given and construct a reverse mapping,
        // i.e. doc -> shard, and create FeatureStore dbs for each shard
        for (String shardIdStr : sourceTopicMap.keySet()) {
            String cPath = dbPath + "/" + SOURCES_DBENV + "/" + shardIdStr;

            // create feature store for shard
            FeatureStore store = new FeatureStore(cPath, true);
            sourcesStores.put(shardIdStr, store);
        }

        // read in the mapping files given and construct a reverse mapping,
        // i.e. doc -> shard, and create FeatureStore dbs for each shard
        for(String topic: topics.keySet()) {
            String shardIdStr = topic.toLowerCase();
            String cPath = dbPath + "/" + TOPICS_DBENV + "/" + shardIdStr;

            // create feature store for shard
            FeatureStore store = new FeatureStore(cPath, false);
            stores.put(shardIdStr, store);

            // store the shard size (# of docs) feature
            double totalDocCount = 0;
            for (String source : topics.get(topic)) {
                totalDocCount += sourcesStores.get(source).getFeature(FeatureStore.SIZE_FEAT_SUFFIX);
            }
            store.putFeature(FeatureStore.SIZE_FEAT_SUFFIX, totalDocCount, (int) totalDocCount);
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
            String term = bytesRef.utf8ToString();
            if (term.isEmpty()) {
                continue;
            }
            termCnt++;
        }

        // add the total term length of shard
        totalTermCount += termCnt;

        IndriFeature indriFeature = new IndriFeature(mu);

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
            if (term.isEmpty()) {
                continue;
            }

            // get term ctf
            double ctf = termEnum.totalTermFreq();

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
            for(String topic: topics.keySet()) {
                String shardIdStr = topic.toLowerCase();
                // don't store empty terms
                if (shardDataMap.get(shardIdStr) != null) {
                    if (shardDataMap.get(shardIdStr).df != 0) {
                        logger.debug(String.format("shard: %s term: %s ctf: %d min: %.2f shardDf: %d f: %.2f f2: %.2f", shardIdStr, term, (int) ctf, shardDataMap.get(shardIdStr).min,
                                (long) shardDataMap.get(shardIdStr).df, shardDataMap.get(shardIdStr).f,
                                shardDataMap.get(shardIdStr).f2));
                        storeTermStats(stores.get(shardIdStr), term, (int) ctf, shardDataMap.get(shardIdStr).min,
                                shardDataMap.get(shardIdStr).df, shardDataMap.get(shardIdStr).f,
                                shardDataMap.get(shardIdStr).f2);
                    }
                }
            }

            // add the total term length of the collection
            totalTermCount += termCnt;
//        totalTermCount = corpusStats.getFeature(FeatureStore.TERM_SIZE_FEAT_SUFFIX);
        } // end term iter

        // clean up
        corpusStore.close();

        for (FeatureStore store : sourcesStores.values()) {
            store.close();
        }

        for (FeatureStore store : stores.values()) {
            store.close();
        }

        indexReader.close();

        logger.info("build sources end");
    }

}
