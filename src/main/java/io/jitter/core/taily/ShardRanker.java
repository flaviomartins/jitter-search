package io.jitter.core.taily;

import cc.twittertools.index.IndexStatuses;
import com.google.common.collect.Lists;
import org.apache.commons.math3.distribution.GammaDistribution;
import io.jitter.core.utils.AnalyzerUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;

public class ShardRanker {
    private static final Logger logger = LoggerFactory.getLogger(ShardRanker.class);

    private final Analyzer analyzer;

    // array of FeatureStore pointers
    // stores[0] is the whole collection store; stores[1] onwards is each shard; length is numShards+1
    private final FeatureStore[] _stores;

    // a single index built the same way; just for stemming term
    private final String indexPath;

    private final String[] _shardIds;

    // number of shards
    private final int _numShards;

    // Taily parameter used in Eq (11)
    private final float _n_c;

    public ShardRanker(List<String> _shardIds, String indexPath, Analyzer analyzer, float _n_c, String dbPath, String shardsDbPath) {
        this(_shardIds.toArray(new String[_shardIds.size()]), indexPath, analyzer, _n_c, dbPath, shardsDbPath);
    }

    public ShardRanker(String[] _shardIds, String indexPath, Analyzer analyzer, float _n_c, String dbPath, String shardsDbPath) {
        this._shardIds = _shardIds;
        this.indexPath = indexPath;
        this.analyzer = analyzer;

        this._n_c = _n_c;
        _numShards = _shardIds.length;

        // open up all output feature storage for each mapping file we are accessing
        _stores = new FeatureStore[_numShards + 1];

        // open collection feature store
        if (new File(dbPath).isDirectory()) {
            FeatureStore collectionStore = new FeatureStore(dbPath, true);
            _stores[0] = collectionStore;
        } else {
            logger.error("directory not found: " + dbPath);
        }

        // read in the mapping files given and construct a reverse mapping,
        // i.e. doc -> shard, and create FeatureStore dbs for each shard
        for (int i = 1; i < _numShards + 1; i++) {
            String shardIdStr = _shardIds[i - 1].toLowerCase(Locale.ROOT);

            // create output directory for the feature store dbs
            String cPath = shardsDbPath + "/" + shardIdStr;

            if (new File(cPath).isDirectory()) {
                // open feature store for shard
                FeatureStore store = new FeatureStore(cPath, true);
                _stores[i] = store;
            } else {
                logger.error("directory not found: " + cPath);
            }
        }
    }

    public String getIndexPath() {
        return indexPath;
    }

    public void close() {
        for (FeatureStore store : _stores) {
            if (store != null) {
                store.close();
            }
        }
    }

    // tokenize, stem and do stopword removal
    private List<String> _getStems(String query) {
        List<String> stems = Lists.newArrayList();
        try {
            Query q = new QueryParser(IndexStatuses.StatusField.TEXT.name, analyzer).parse(query);
            Set<Term> queryTerms = new LinkedHashSet<>();
            q.extractTerms(queryTerms);
            for (Term term : queryTerms) {
                stems.add(term.text());
            }
        } catch (ParseException e) {
            stems = AnalyzerUtils.analyze(analyzer, query);
        }
        int len = stems.size();
        // remove empty stems
        stems.removeAll(Arrays.asList("", null));
        if (len > stems.size()) {
            logger.warn("Empty terms were found and removed automatically. Check your tokenizer.");
        }
        return stems;
    }

    public int getDF(String shardId, String stem) {
        if (stem.isEmpty()) {
            logger.warn("Tryed to get the DF of an empty term. Will return numDocs instead.");
        }
        int i = Lists.newArrayList(_shardIds).indexOf(shardId.toLowerCase(Locale.ROOT)) + 1;
        if (i > 0) {
            // get term's shard df
            String dfFeat = stem + FeatureStore.SIZE_FEAT_SUFFIX;
            double df = _stores[i].getFeature(dfFeat);
            if (df > 0)
                return (int) df;
        }
        return 1;
    }

    static class QueryFeats {

        final double[] queryMean;
        final double[] queryVar;
        final boolean[] hasATerm;
        final double[] dfTerm;

        public QueryFeats(int numShards) {
            queryMean = new double[numShards];
            queryVar = new double[numShards];
            hasATerm = new boolean[numShards];
            dfTerm = new double[numShards];
        }
    }

    // retrieves the mean/variance for query terms and fills in the given queryMean/queryVar arrays
    // and marks shards that have at least one doc for one query term in given bool array
    private QueryFeats _getQueryFeats(List<String> stems) {
        QueryFeats queryFeats = new QueryFeats(_numShards + 1);
        // calculate mean and variances for query for all shards
        for (String stem : stems) {
            if (stem.isEmpty()) {
                logger.warn("Got empty stem");
                continue;
            }

            // get minimum doc feature value for this stem
            String minFeat = stem + FeatureStore.MIN_FEAT_SUFFIX;
            double minVal = _stores[0].getFeature(minFeat);

            boolean calcMin = false;
            if (minVal == -1) {
                minVal = Double.MAX_VALUE;
                calcMin = true;
            }

            // sums of individual shard features to calculate corpus-wide feature
            double globalFSum = 0;
            double globalF2Sum = 0;
            double globalDf = 0;

            // keep track of how many times this term appeared in the shards for minVal later
            double[] dfCache = new double[_numShards + 1];

            // for each shard (not including whole corpus db), calculate mean/var
            // keep track of totals to use in the corpus-wide features
            for (int i = 1; i < _numShards + 1; i++) {
                // get current term's shard df
                String dfFeat = stem + FeatureStore.SIZE_FEAT_SUFFIX;
                double df = _stores[i].getFeature(dfFeat);

                // TODO: fix this kludge
                // if this shard doesn't have this term, skip; otherwise you get nan everywhere
                if (df == -1)
                    continue;

                dfCache[i] = df;
                globalDf += df;

                queryFeats.hasATerm[i] = true;
                queryFeats.dfTerm[i] += df;
                queryFeats.dfTerm[0] += df;

                // add current term's mean to shard; also shift by min feat value Eq (5)
                String meanFeat = stem + FeatureStore.FEAT_SUFFIX;
                double fSum = _stores[i].getFeature(meanFeat);
                if (fSum == -1) {
                    logger.error("BAD fSum");
                }
                //queryFeats.queryMean[i] += fSum / df - minVal;
                queryFeats.queryMean[i] += fSum / df; // handle min values separately afterwards
                globalFSum += fSum;

                // add current term's variance to shard Eq (6)
                String f2Feat = stem + FeatureStore.SQUARED_FEAT_SUFFIX;
                double f2Sum = _stores[i].getFeature(f2Feat);
                if (f2Sum < 0) {
                    logger.error("BAD f2Sum");
                }

                queryFeats.queryVar[i] += (f2Sum / df) - Math.pow(fSum / df, 2);
                globalF2Sum += f2Sum;

                // if there is no global min stored, figure out the minimum from shards
                if (calcMin) {
                    double currMin = _stores[i].getFeature(minFeat);
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
                    // FIXME: removes shard corresponding to minVal?
                    queryFeats.queryMean[i] -= minVal;
                }
            }
        }
        return queryFeats;
    }

    // calculates All from Eq (10)
    private double[] _getAll(List<String> stems) {
        // calculate Any_i & all_i
        double[] all = new double[_numShards + 1];
        double[] any = new double[_numShards + 1];

        for (int i = 0; i < _numShards + 1; i++) {
            // initialize Any_i & all_i
            any[i] = 1.0;
            all[i] = 0.0;

            // get size of current shard
            double shardSize = _stores[i].getFeature(FeatureStore.SIZE_FEAT_SUFFIX);

            // for each query term, calculate inner bracket of any_i equation
            double[] dfs = new double[stems.size()];
            int dfCnt = 0;
            for (String stem : stems) {
                String stemKey = stem + FeatureStore.SIZE_FEAT_SUFFIX;
                double df = _stores[i].getFeature(stemKey);

                // no smoothing
                if (df == -1) {
                    df = 0;
                }

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
        Map<String, Double> ranking = new HashMap<>();

        List<String> stems = _getStems(query);

        QueryFeats queryFeats = _getQueryFeats(stems);
        // query total means and variances for each shard
        double[] queryMean = queryFeats.queryMean;
        double[] queryVar = queryFeats.queryVar;
        // to mark shards that have at least one doc for one query term
        boolean[] hasATerm = queryFeats.hasATerm;
        // used in the ranking for var ~= 0 cases
        double[] dfTerm = queryFeats.dfTerm;

        // fast fall-through for 2 degenerate cases
        if (!hasATerm[0]) {
            // case 1: there are no documents in the entire collection that matches any query term
            // return empty ranking
            return ranking;
        } else if (queryVar[0] < 1e-10) {
            // case 2: there is only 1 document in entire collection that matches any query term
            // return the shard with the document with n_i = 1

            // these var ~= 0 cases should be handled carefully; instead of n_i = 1,
            // it could be there are two or more very similarly scoring docs; We keep track
            // of the df of these shards and use that instead of n_i = 1.
            for (int i = 1; i < _numShards + 1; i++) {
                if (hasATerm[i]) {
                    ranking.put(_shardIds[i - 1], dfTerm[i]);
                } else {
                    ranking.put(_shardIds[i - 1], 0.0);
                }
            }
            return sortAndNormalization(ranking);
        }

        // all from Eq (10)
        double[] all = _getAll(stems);

        // fast fall-through for for 1 degenerate case
        if (all[0] < 1e-10) {
            // if all[0] is ~= 0, then all[i] is ~= 0 because no shard contains all of the query terms
            // these all[0] ~= 0 cases should be handled carefully; instead of just using queryMean,
            // it could be more effective calculating *all* again for the maximum number of query terms
            for (int i = 1; i < _numShards + 1; i++) {
                if (hasATerm[i]) {
                    // actually use mean of the shard as score
                    ranking.put(_shardIds[i - 1], queryMean[i]);
                } else {
                    ranking.put(_shardIds[i - 1], 0.0);
                }
            }
            return sortAndNormalization(ranking);
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

        // calculate s_c from inline equation after Eq (11)
        double p_c = _n_c / all[0];

        // if n_c > all[0], set probability to 1
        if (p_c >= 1.0)
            p_c = 1.0;

        GammaDistribution collectionGamma = new GammaDistribution(k[0], theta[0]);
        double s_c = collectionGamma.inverseCumulativeProbability(1.0 - p_c);

        // calculate n_i for all shards and store it in ranking vector so we can sort (not normalized)
        for (int i = 1; i < _numShards + 1; i++) {
            // if there are no query terms in shard, skip
            if (!hasATerm[i]) {
                ranking.put(_shardIds[i - 1], 0.0);
                continue;
            }

            // if var is ~= 0, then don't build a distribution.
            // based on the mean of the shard (which is the score of the single doc), n_i is either 0 or 1
            if (queryVar[i] < 1e-10) {
                // actually use mean of the shard as score
                ranking.put(_shardIds[i - 1], queryMean[i]);
            } else {
                // do normal Taily stuff pre-normalized Eq (12)
                GammaDistribution shardGamma = new GammaDistribution(k[i], theta[i]);
                double p_i = 1.0 - shardGamma.cumulativeProbability(s_c);
                ranking.put(_shardIds[i - 1], all[i] * p_i);
            }
        }

        return sortAndNormalization(ranking);
    }

    private TreeMap<String, Double> sortAndNormalization(Map<String, Double> ranking) {
        // sort shards by n
        ShardComparator comparator = new ShardComparator(ranking);
        TreeMap<String, Double> sortedMap = new TreeMap<>(comparator);
        sortedMap.putAll(ranking);

        double sum = 0;
        for (Map.Entry<String, Double> entry : sortedMap.entrySet()) {
            sum += entry.getValue();
        }

        // if sum is 0 no shard will be selected
        if (sum == 0) {
            logger.error("BAD sum");
        }

        double norm = _n_c / sum;

        // normalize shard scores Eq (12)
        TreeMap<String, Double> normedMap = new TreeMap<>(comparator);
        for (Map.Entry<String, Double> entry : sortedMap.entrySet()) {
            normedMap.put(entry.getKey(), entry.getValue() * norm);
        }

        return normedMap;
    }

}
