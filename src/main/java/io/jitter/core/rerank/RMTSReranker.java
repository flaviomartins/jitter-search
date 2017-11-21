package io.jitter.core.rerank;

import ciir.umass.edu.features.LinearNormalizer;
import ciir.umass.edu.learning.DataPoint;
import ciir.umass.edu.learning.RankList;
import ciir.umass.edu.learning.Ranker;
import ciir.umass.edu.learning.RankerFactory;
import ciir.umass.edu.utilities.MergeSorter;
import com.google.common.collect.Lists;
import com.twitter.Extractor;
import io.jitter.api.collectionstatistics.CollectionStats;
import io.jitter.api.search.StatusDocument;
import io.jitter.core.analysis.TweetAnalyzer;
import io.jitter.core.document.DocVector;
import io.jitter.core.features.BM25Feature;
import io.jitter.core.probabilitydistributions.KDE;
import io.jitter.core.utils.AnalyzerUtils;
import io.jitter.core.utils.ListUtils;
import io.jitter.core.utils.TimeUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class RMTSReranker implements Reranker {

    private static final Logger logger = LoggerFactory.getLogger(RMTSReranker.class);

    private static final double DAY = 60.0 * 60.0 * 24.0;

    private static final Analyzer ANALYZER = new TweetAnalyzer();
    private static final TFIDFSimilarity SIMILARITY = new DefaultSimilarity();

    private final String rankerModel;
    private final String query;
    private final double queryEpoch;
    private List<StatusDocument> shardResults;
    private final CollectionStats collectionStats;
    private final int numResults;
    private final int numRerank;
    private final boolean rank;

    public RMTSReranker(String rankerModel, String query, double queryEpoch, List<StatusDocument> shardResults, CollectionStats collectionStats, int numResults, int numRerank, boolean rank) {
        this.rankerModel = rankerModel;
        this.query = query;
        this.queryEpoch = queryEpoch;
        this.shardResults = shardResults;
        this.collectionStats = collectionStats;
        this.numResults = numResults;
        this.numRerank = numRerank;
        this.rank = rank;
    }

    @Override
    public List<StatusDocument> rerank(List<StatusDocument> results, RerankerContext context) {
        Set<String> qTerms = new LinkedHashSet<>();
        for (String term : AnalyzerUtils.analyze(ANALYZER, query)) {
            if (!term.isEmpty()) {
                qTerms.add(term);
            }
        }

        BM25Feature bm25Feature = new BM25Feature(1.2D, 0.75D);
        Extractor extractor = new Extractor();

        for (StatusDocument result : results) {
            result.getFeatures().add((float) result.getRsv());
        }

        double lambda = 0.01;
        RecencyReranker reranker = new RecencyReranker(lambda);
        results = reranker.rerank(results, context);

        KDE.METHOD method = KDE.METHOD.REFLECTION;
//        if (kdeMethod != null) {
//            method = KDE.METHOD.valueOf(kdeMethod);
//        }

        for (StatusDocument result : results) {
            result.getFeatures().add(0f);
        }
//        KDEReranker kdeReranker = new KDEReranker(results, queryEpoch, method, KDEReranker.WEIGHT.UNIFORM, 1.0);
//        results = kdeReranker.getReranked();
        KDEReranker kdeReranker1 = new KDEReranker(results.subList(0, Math.min(results.size(), numRerank)), method, KDEReranker.WEIGHT.SCORE, 1.0);
        results = kdeReranker1.rerank(results, context);

        for (StatusDocument result : results) {
            result.getFeatures().add(0f);
            result.getFeatures().add(0f);
//            result.getFeatures().add(0f);
        }

//        KDERerank(viewsOracle, query, method, bViews);
//        KDERerank(editsOracle, query, method, bEdits);

        // KDE News
        if (shardResults.size() > 1) {
            KDEReranker kdeReranker2 = new KDEReranker(shardResults, method, KDEReranker.WEIGHT.SCORE, 1.0);
            results = kdeReranker2.rerank(results, context);
        } else {
            for (StatusDocument result : results) {
                result.getFeatures().add(0f);
            }
        }

        int numDocs = collectionStats.numDocs();
        HashMap<String, Integer> dfs = new HashMap<>();
        for (String term : qTerms) {
            int docFreq = collectionStats.docFreq(term);
            dfs.put(term, docFreq);
        }

        for (StatusDocument result : results) {
            double averageDocumentLength = 28;
            double docLength;
            double idf = 0;
            double bm25 = 0;
            double coord = 0;
            double tfMax = 0;
            double tfTotal = 0;
            double tfMean;

            DocVector docVector = result.getDocVector();
            // if the term vectors are unavailable generate it here
            if (docVector == null) {
                DocVector aDocVector = new DocVector();
                List<String> docTerms = AnalyzerUtils.analyze(ANALYZER, result.getText());

                for (String t : docTerms) {
                    if (!t.isEmpty()) {
                        Integer n = aDocVector.vector.get(t);
                        n = (n == null) ? 1 : ++n;
                        aDocVector.vector.put(t, n);
                    }
                }

                docVector = aDocVector;
            }

            docLength = docVector.getLength();

            for (Map.Entry<String, Integer> tf : docVector.vector.entrySet()) {
                String term = tf.getKey();
                if (qTerms.contains(term)) {
                    double tfValue = tf.getValue();
                    if (tfValue > 0) {
                        int docFreq = dfs.get(term);
                        idf += SIMILARITY.idf(docFreq, numDocs);
                        bm25 += bm25Feature.value(tfValue, docLength, averageDocumentLength, docFreq, numDocs);
                        coord += 1;
                        tfMax = Math.max(tfMax, tfValue);
                        tfTotal += tfValue;
                    }
                }
            }
            tfMean = tfTotal / coord;

            result.getFeatures().add((float) idf);

            result.getFeatures().add((float) coord);

            result.getFeatures().add((float) docLength);

            float numURLs = 0;
            float numHashtags = 0;
            float numMentions = 0;

            List<Extractor.Entity> entityList = extractor.extractEntitiesWithIndices(result.getText());
            for (Extractor.Entity entity : entityList) {
                switch (entity.getType()) {
                    case URL:
                        numURLs++;
                        break;
                    case HASHTAG:
                        numHashtags++;
                        break;
                    case MENTION:
                        numMentions++;
                        break;
                    default:
                }
            }

            result.getFeatures().add(numURLs);

            if (numURLs > 0) {
                result.getFeatures().add(1.0f);
            } else {
                result.getFeatures().add(0.0f);
            }

            result.getFeatures().add(numHashtags);

            if (numHashtags > 0) {
                result.getFeatures().add(1.0f);
            } else {
                result.getFeatures().add(0.0f);
            }

            result.getFeatures().add(numMentions);

            if (numMentions > 0) {
                result.getFeatures().add(1.0f);
            } else {
                result.getFeatures().add(0.0f);
            }

            long in_reply_to_status_id = result.getIn_reply_to_status_id();
            if (in_reply_to_status_id > 0) {
                result.getFeatures().add(1.0f);
            } else {
                result.getFeatures().add(0.0f);
            }

            result.getFeatures().add((float) Math.log(1 + result.getStatuses_count()));

            result.getFeatures().add((float) Math.log(1 + result.getFollowers_count()));

            result.getFeatures().add((float) bm25);

            result.getFeatures().add((float) tfTotal);
        }

        if (rank && results.size() > 0) {
            try {
                RankerFactory rFact = new RankerFactory();
                Ranker ranker = rFact.loadRankerFromFile(rankerModel);
                results = rankRankLib(ranker, query, results, numResults);
            } catch (SecurityException e) {
                logger.warn("RankLib caught calling System.exit(int).");
            }
        }
        return results;
    }

    private List<StatusDocument> KDERerank(List<Double> oracleRawEpochs, List<Double> oracleWeights, List<StatusDocument> results, double queryEpoch, KDE.METHOD method, double weight, RerankerContext context) {
        // if we're using our oracle, we need the right training data
        List<Double> oracleScaledEpochs = TimeUtils.adjustEpochsToLandmark(oracleRawEpochs, queryEpoch, DAY);
        double[] densityTrainingData = ListUtils.listToArray(oracleScaledEpochs);
        double[] densityWeights = ListUtils.listToArray(oracleWeights);
//        Arrays.fill(densityWeights, 1.0 / (double) densityWeights.length);

        KernelDensityReranker kernelDensityReranker = new KernelDensityReranker(densityTrainingData, densityWeights, method, weight);
        return kernelDensityReranker.rerank(results, context);
    }

    @SuppressWarnings("UnusedAssignment")
    private List<StatusDocument> rankRankLib(Ranker ranker, String query, List<StatusDocument> results, int numResults) {
        int[] features = ranker.getFeatures();
        List<DataPoint> rl = new ArrayList<>();

        String qid = query.replaceFirst("^MB0*", "");

        int i = 1;
        for (StatusDocument hit : results) {
//            String rel = String.valueOf(qrels.getRel(qid, String.valueOf(hit.getId())));
//            DataPoint dp = hit.getDataPoint(rel, qid);
            DataPoint dp = hit.getDataPoint();
            rl.add(dp);
        }
        RankList l = new RankList(rl);

        LinearNormalizer nml = new LinearNormalizer();
        nml.normalize(l, features);

        double[] scores = new double[l.size()];
        for (int j = 0; j < l.size(); j++)
            scores[j] = ranker.eval(l.get(j));


        List<StatusDocument> finalResults = Lists.newArrayList();
        int[] idx = MergeSorter.sort(scores, false);
        for (int j = 0; j < Math.min(idx.length, numResults); j++) {
            int k = idx[j];
            String docno = l.get(k).getDescription();
//            String rel = qrels.getRelString(qid, docno);
            String rel = "0";

//            float maxTF = l.get(k).getFeatureValue(21);
//            if (maxTF < 3) {
                StatusDocument updatedResult = new StatusDocument(results.get(k));
                updatedResult.setRsv(scores[k]);
                finalResults.add(updatedResult);
//            }

//            System.out.println(String.format("%s Q0 %s %d %." + (int) (6 + Math.ceil(Math.log10(numResults))) + "f RUN # rel = %s, rt = %s, text = %s", qid, docno, (j + 1),
//                    scores[k], rel, results.get(j).getRetweeted_status_id(), results.get(j).getText().replaceAll("\\r?\\n", " --linebreak-- ")));
        }
        return finalResults;
    }
}
