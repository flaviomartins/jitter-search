package org.novasearch.jitter.core.selection.taily;

import cc.twittertools.index.TweetAnalyzer;
import org.apache.commons.math3.distribution.GammaDistribution;
import org.apache.log4j.Logger;
import org.apache.lucene.util.Version;
import org.novasearch.jitter.utils.AnalyzerUtils;

import java.util.*;

public class ShardRanker {
    private static final Logger logger = Logger.getLogger(ShardRanker.class);

    // array of FeatureStore pointers
    // stores[0] is the whole collection store; stores[1] onwards is each shard; length is numShards+1
    private final FeatureStore[] _stores;

    // a single index built the same way; just for stemming term
    private final String indexPath;

    private String[] _shardIds;

    // number of shards
    private final int _numShards;

    // Taily parameter used in Eq (11)
    private final int _n_c;

    public ShardRanker(List<String> _shardIds, String indexPath, int _n_c) {
        this(_shardIds.toArray(new String[_shardIds.size()]), indexPath, _n_c);
    }

    public ShardRanker(String[] _shardIds, String indexPath, int _n_c) {
        this._shardIds = _shardIds;
        this.indexPath = indexPath;
        this._n_c = _n_c;
        _numShards = _shardIds.length;

        // open up all output feature storages for each mapping file we are accessing
        _stores = new FeatureStore[_numShards + 1];

        // read collection feature store
        String dbPath = "bdb";
        FeatureStore collectionStore = new FeatureStore(dbPath, true);
        _stores[0] = collectionStore;

        dbPath = "bdbmap"; // in this case, this will be a path to a folder

        // read in the mapping files given and construct a reverse mapping,
        // i.e. doc -> shard, and create FeatureStore dbs for each shard
        for (int i = 1; i < _numShards + 1; i++) {
            String shardIdStr = _shardIds[i - 1];

            // create output directory for the feature store dbs
            String cPath = dbPath + "/" + shardIdStr;

            // create feature store for shard
            FeatureStore store = new FeatureStore(cPath, false);
            _stores[i] = store;
        }
    }

    public void close() {
        for (FeatureStore store : _stores) {
            store.close();
        }
    }

    private List<String> _getStems(String query) {
        return AnalyzerUtils.analyze(new TweetAnalyzer(Version.LUCENE_43, true), query);
    }

    class QueryFeats {

        double[] queryMean;
        double[] queryVar;
        boolean[] hasATerm;

        public QueryFeats(int numShards) {
            queryMean = new double[numShards];
            queryVar = new double[numShards];
            hasATerm = new boolean[numShards];
        }
    }

    private QueryFeats _getQueryFeats(List<String> stems) {
        QueryFeats queryFeats = new QueryFeats(_numShards + 1);
        // calculate mean and variances for query for all shards
        for (String stem : stems) {
            // get minimum doc feature value for this stem
            double minVal = Double.MAX_VALUE;
            String minFeat = stem + FeatureStore.MIN_FEAT_SUFFIX;
            minVal = _stores[0].getFeature(minFeat);

            boolean calcMin = false;
            if (minVal == -1) {
                calcMin = true;
            }

            // sums of individual shard features to calculate corpus-wide feature
            double globalFSum = 0;
            double globalF2Sum = 0;
            double globalDf = 0;

            // keep track of how many times this term appeared in the shards for minVal later
            double[] dfCache = new double[_numShards +1];
            for(int i = 0; i <  + 1; i++) {
                dfCache[i] = 0;
            }

            // for each shard (not including whole corpus db), calculate mean/var
            // keep track of totals to use in the corpus-wide features
            for (int i = 1; i < _numShards + 1; i++) {
                // get current term's shard df
                double df = 0;
                String dfFeat = stem + FeatureStore.SIZE_FEAT_SUFFIX;
                df = _stores[i].getFeature(dfFeat);
                dfCache[i] = df;
                globalDf += df;

                // if this shard doesn't have this term, skip; otherwise you get nan everywhere
                if (df == 0)
                    continue;
                queryFeats.hasATerm[i] = true;

                // add current term's mean to shard; also shift by min feat value Eq (5)
                double fSum = 0;
                String meanFeat = stem + FeatureStore.FEAT_SUFFIX;
                fSum = _stores[i].getFeature(meanFeat);
                //queryMean[i] += fSum/df - minVal;
                queryFeats.queryMean[i] += fSum / df; // handle min values separately afterwards
                globalFSum += fSum;

                // add current term's variance to shard Eq (6)
                double f2Sum = 0;
                String f2Feat = stem + FeatureStore.SQUARED_FEAT_SUFFIX;
                f2Sum = _stores[i].getFeature(f2Feat);
                queryFeats.queryVar[i] += f2Sum / df - Math.pow(fSum / df, 2);
                globalF2Sum += f2Sum;

                // if there is no global min stored, figure out the minimum from shards
                if (calcMin) {
                    double currMin;
                    currMin = _stores[i].getFeature(minFeat);
                    if (currMin < minVal) {
                        minVal = currMin;
                    }
                }
            }

            dfCache[0] = globalDf;
            if (globalDf > 0) {
                queryFeats.hasATerm[0] = true;

                // calculate global mean/variances based on shard sums; again, minVal is for later
                queryFeats.queryMean[0] += globalFSum / globalDf;
                queryFeats.queryVar[0] += globalF2Sum / globalDf - Math.pow(globalFSum / globalDf, 2);
            }

            // adjust shard mean by minimum value
            for (int i = 0; i < _numShards + 1; i++) {
                if (dfCache[i] > 0) {
                    queryFeats.queryMean[i] -= minVal;
                }
            }
        }
        return queryFeats;
    }

    private double[] _getAll(List<String> stems) {
        // calculate Any_i & all_i
        double[] all = new double[_numShards + 1];
        double[] any = new double[_numShards + 1];
        String sizeKey = FeatureStore.SIZE_FEAT_SUFFIX;

        for (int i = 0; i < _numShards + 1; i++) {
            // initialize Any_i & all_i
            any[i] = 1.0;
            all[i] = 0.0;

            // get size of current shard
            double shardSize;
            shardSize = _stores[i].getFeature(sizeKey);

            // for each query term, calculate inner bracket of any_i equation
            double[] dfs = new double[stems.size()];
            int dfCnt = 0;
            for (String stem : stems) {
                String stemKey = stem + FeatureStore.SIZE_FEAT_SUFFIX;
                double df;
                df = _stores[i].getFeature(stemKey);

                // smooth it
                if (df < 5)
                    df = 5;

                // store df for all_i calculation
                dfs[dfCnt++] = df;

                any[i] *= (1 - df / shardSize);
            }

            // calculation of any_i
            any[i] = shardSize * (1 - any[i]);

            // calculation of all_i Eq (10)
            all[i] = any[i];
            for (int j = 0; j < stems.size(); j++) {
                all[i] *= dfs[j] / any[i];
            }

        }
        return all;
    }

    public Map<String, Double> rank(String query) {
        // +1 because 0 stands for central db
        // query total means and variances for each shard
        double[] queryMean;
        double[] queryVar;
        boolean[] hasATerm; // to mark shards that have at least one doc for one query term

        Map<String, Double> ranking = new HashMap<>();

        List<String> stems = _getStems(query);

        QueryFeats queryFeats = _getQueryFeats(stems);
        queryMean = queryFeats.queryMean;
        queryVar = queryFeats.queryVar;
        hasATerm = queryFeats.hasATerm;

        // fast fall-through for 2 degenerate cases
        if (!hasATerm[0]) {
            // case 1: there are no documents in the entire collection that matches any query term
            // return empty ranking
            return ranking;
        } else if (queryVar[0] < 1e-10) {
            // FIXME: these var ~= 0 cases should really be handled more carefully; instead of
            // n_i = 1, it could be there are two or more very similarly scoring docs; I should keep
            // track of the df of these shards and use that instead of n_i = 1...

            // case 2: there is only 1 document in entire collection that matches any query term
            // return the shard with the document with n_i = 1
            for (int i = 1; i < _numShards + 1; i++) {
                if (hasATerm[i]) {
                    ranking.put(_shardIds[i - 1], 1d);
                    break;
                }
            }
            return ranking;
        }

        // calculate k and theta from mean/vars Eq (7) (8)
        double[] k = new double[_numShards + 1];
        double[] theta = new double[_numShards + 1];

        for (int i = 0; i < _numShards + 1; i++) {
            // special case, if df = 1, then var ~= 0 (or if no terms occur in shard)
            if (queryVar[i] < 1e-10) {
                k[i] = -1;
                theta[i] = -1;
                continue;
            }

            k[i] = Math.pow(queryMean[i], 2) / queryVar[i];
            theta[i] = queryVar[i] / queryMean[i];
        }

        // all from Eq (10)
        double[] all = _getAll(stems);

        // calculate s_c from inline equation after Eq (11)
        double p_c = _n_c / all[0];

        // if n_c > all[0], set probability to 1
        if (p_c > 1.0)
            p_c = 0.99; // ZOMG

        GammaDistribution collectionGamma = new GammaDistribution(k[0], theta[0]);
        double s_c = collectionGamma.inverseCumulativeProbability(p_c);

        // calculate n_i for all shards and store it in ranking vector so we can sort (unnormalized)
        for (int i = 1; i < _numShards + 1; i++) {
            // if there are no query terms in shard, skip
            if (!hasATerm[i])
                continue;

            // if var is ~= 0, then don't build a distribution.
            // based on the mean of the shard (which is the score of the single doc), n_i is either 0 or 1
            if (queryVar[i] < 1e-10 && hasATerm[i]) {
                if (queryMean[i] >= s_c) {
                    ranking.put(_shardIds[i - 1], 1d);
                }
            } else {
                // do normal Taily stuff pre-normalized Eq (12)
                GammaDistribution shardGamma = new GammaDistribution(k[i], theta[i]);
                double p_i = 1.0d - shardGamma.cumulativeProbability(s_c);
                ranking.put(_shardIds[i - 1], all[i] * p_i);
            }
        }

        // sort shards by n
        ShardComparator comparator = new ShardComparator(ranking);
        TreeMap<String, Double> sortedMap = new TreeMap<>(comparator);
        sortedMap.putAll(ranking);

        // get normalization factor (top 5 shards sufficient)
        double sum = 0.0;
        int i = 0;
        int limit = Math.min(5, ranking.size());
        for (Map.Entry<String, Double> entry : sortedMap.entrySet()) {
            sum += entry.getValue();
            if (i > limit)
                break;
        }
        double norm = _n_c / sum;

        // normalize shard scores Eq (12)
        TreeMap<String, Double> normedMap = new TreeMap<>(comparator);
        for (Map.Entry<String, Double> entry : sortedMap.entrySet()) {
            normedMap.put(entry.getKey(), entry.getValue() * norm);
        }

        return normedMap;
    }

    public static void main(String[] args) {
        List<String> screenNames = new ArrayList<>();
        screenNames.add("ap");
        screenNames.add("reuters");
        screenNames.add("bbcworld");
        screenNames.add("usatoday");
        ShardRanker ranker = new ShardRanker(screenNames.toArray(new String[screenNames.size()]), "/home/fmartins/Data/cmu/twitter/collectionindex", 400);


        Map<String, Double> rank = ranker.rank("chuck hagel");
        for (Map.Entry<String, Double> entry : rank.entrySet()) {
            System.out.println(entry.getKey() + " " + entry.getValue());
        }

        Scanner sc = new Scanner(System.in);
        while (sc.hasNext()) {
            String query = sc.nextLine();
            rank = ranker.rank(query);
            for (Map.Entry<String, Double> entry : rank.entrySet()) {
                System.out.println(entry.getKey() + " " + entry.getValue());
            }
        }

    }
}
