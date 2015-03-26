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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Taily {
    private static final Logger logger = Logger.getLogger(Taily.class);

    private static final int DEFAULT_MU = 2500;

    private String indexPath;

    public Taily(String indexPath) {
        this.indexPath = indexPath;
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

    public void buildCorpus() throws IOException {
        //TODO: read db path
        String dbPath = "bdb";

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
        double ctf = termEnum.totalTermFreq();
        double df = termEnum.docFreq();

        // store df feature for term
        String dfFeatKey = termEnum.term().utf8ToString() + FeatureStore.SIZE_FEAT_SUFFIX;
        store.addValFeature(dfFeatKey, df, (int) ctf);
        logger.debug(dfFeatKey + " " + df);

        // store ctf feature for term
        String ctfFeatKey = termEnum.term().utf8ToString() + FeatureStore.TERM_SIZE_FEAT_SUFFIX;
        store.addValFeature(ctfFeatKey, ctf, (int) ctf);
        logger.debug(ctfFeatKey + " " + ctf);
    }

    public void buildFromMap(List<String> screenNames) throws IOException {
        String dbPath = "bdbmap"; // in this case, this will be a path to a folder

        // TODO: read terms list

        DirectoryReader indexReader = DirectoryReader.open(FSDirectory.open(new File(indexPath)));

        // open up all output feature storages for each mapping file we are accessing
        Map<String, FeatureStore> stores = new HashMap<>();

        // read in the mapping files given and construct a reverse mapping,
        // i.e. doc -> shard, and create FeatureStore dbs for each shard
        for(String shardIdStr: screenNames) {

            // create output directory for the feature store dbs
            String cPath = dbPath+"/"+shardIdStr;
            if (new File(cPath).mkdir() != true) {
                logger.warn("Error creating output DB dir. Dir may already exist.");
//                System.exit(-1);
            }

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
            if (termCnt % 100 == 0) {
                logger.info("  Finished " + termCnt + " terms");
            }

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

                        double length = doc.get(IndexStatuses.StatusField.TEXT.name).split(" ").length;
                        double tf = docsEnum.freq();

                        // calulate Indri score feature and sum it up
                        double feat = calcIndriFeature(tf, ctf, totalTermCount, length, DEFAULT_MU);

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
                        logger.debug(String.format("shard: %s term: %s ctf: %d min: %.2f shardDf: %d f: %.2f f2: %.2f", shardIdStr, bytesRef.utf8ToString(), (int) ctf, shardDataMap.get(shardIdStr).min,
                                (long) shardDataMap.get(shardIdStr).shardDf, shardDataMap.get(shardIdStr).f,
                                shardDataMap.get(shardIdStr).f2));
                        storeTermStats(stores.get(shardIdStr), bytesRef.utf8ToString(), (int) ctf, shardDataMap.get(shardIdStr).min,
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

    public static void main(String[] args) throws IOException {
        Taily taily = new Taily("/home/fmartins/Data/cmu/twitter/collectionindex");
//        taily.buildCorpus();

//        String dbPath = "bdb";
//        // TODO: read terms list
//        FeatureStore store = new FeatureStore(dbPath, false);
//        System.out.println(store.getFeature("#t"));

        String dbPath = "bdbmap";
        List<String> screenNames = new ArrayList<>();
        screenNames.add("ap");
        screenNames.add("reuters");
        screenNames.add("bbcworld");
        screenNames.add("usatoday");
        taily.buildFromMap(screenNames);
    }

}
