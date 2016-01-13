package io.jitter.core.rerank;

import cc.twittertools.index.TweetAnalyzer;
import ciir.umass.edu.features.LinearNormalizer;
import ciir.umass.edu.learning.DataPoint;
import ciir.umass.edu.learning.RankList;
import ciir.umass.edu.learning.Ranker;
import ciir.umass.edu.learning.RankerFactory;
import ciir.umass.edu.utilities.MergeSorter;
import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import com.twitter.Extractor;
import io.jitter.api.collectionstatistics.CollectionStats;
import io.jitter.api.search.Document;
import io.jitter.core.features.BM25Feature;
import io.jitter.core.probabilitydistributions.KDE;
import io.jitter.core.probabilitydistributions.LocalExponentialDistribution;
import io.jitter.core.twittertools.api.TResultWrapper;
import io.jitter.core.utils.AnalyzerUtils;
import io.jitter.core.utils.TimeUtils;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class RMTSReranker extends SearchReranker {
    private static final Logger logger = LoggerFactory.getLogger(RMTSReranker.class);

    private static final double DAY = 60.0 * 60.0 * 24.0;

    private final String query;
    private final double queryEpoch;
    private final CollectionStats collectionStats;
    private final int numResults;
    private final int numRerank;

    public RMTSReranker(String query, double queryEpoch, List<Document> results, CollectionStats collectionStats, int numResults, int numRerank) {
        this.query = query;
        this.queryEpoch = queryEpoch;
        this.results = results;
        this.collectionStats = collectionStats;
        this.numResults = numResults;
        this.numRerank = numRerank;
        this.score();
    }

    protected void score() {
        for (Document result : results) {
            result.getFeatures().add((float) result.getRsv());
        }

        // extract raw epochs from results
        List<Double> rawEpochs = TimeUtils.extractEpochsFromResults(results);
        // groom our hit times wrt to query time
        List<Double> scaledEpochs = TimeUtils.adjustEpochsToLandmark(rawEpochs, queryEpoch, DAY);

        double lambda = 0.01;

        RecencyReranker reranker = new RecencyReranker(results, new LocalExponentialDistribution(lambda), scaledEpochs);
        results = reranker.getReranked();

        KDE.METHOD method = KDE.METHOD.STANDARD;
//        if (kdeMethod != null) {
//            method = KDE.METHOD.valueOf(kdeMethod);
//        }

        for (Document result : results) {
            result.getFeatures().add(0f);
        }
//        KDEReranker kdeReranker = new KDEReranker(results, queryEpoch, method, KDEReranker.WEIGHT.UNIFORM, 1.0);
//        results = kdeReranker.getReranked();
        KDEReranker kdeReranker1 = new KDEReranker(results, queryEpoch, method, KDEReranker.WEIGHT.SCORE, 1.0);
        results = kdeReranker1.getReranked();

//        KDERerank(retrievalOracle, query, method, bRet); // USE KDE RERANKER
        for (Document result : results) {
            result.getFeatures().add(0f);
            result.getFeatures().add(0f);
            result.getFeatures().add(0f);
        }
//        KDERerank(viewsOracle, query, method, bViews);
//        KDERerank(editsOracle, query, method, bEdits);
//        KDERerank(newsOracle, query, method, bNews);

        BM25Feature bm25Feature = new BM25Feature(1.2D, 1.0D);

        Extractor extractor = new Extractor();

        List<String> queryTerms = AnalyzerUtils.analyze(new TweetAnalyzer(Version.LUCENE_43, true), query);

        for (TResultWrapper result : results) {
            List<String> terms = AnalyzerUtils.analyze(new TweetAnalyzer(Version.LUCENE_43, true), result.getText());

            double[] tfValues = new double[queryTerms.size()];

            for (int i = 0; i < queryTerms.size(); i++) {
                String tq = queryTerms.get(i);
                for (String td : terms) {
                    if (tq.equals(td)) {
                        tfValues[i]++;
                    }
                }
            }

            double docLength = (double) terms.size();
            double averageDocumentLength = 28;

            double idf = 0;
            double bm25 = 0;
            double cnt = 0;

            for (int i = 0; i < tfValues.length; i++) {
                double value = tfValues[i];
                if (value > 0) {
                    idf += collectionStats.getIDF(queryTerms.get(i));
                    bm25 += bm25Feature.value(tfValues[i], docLength, averageDocumentLength, collectionStats.getDF(queryTerms.get(i)), collectionStats.getCollectionSize());
                    cnt += 1;
                }
            }

            result.getFeatures().add((float) idf);

            result.getFeatures().add((float) cnt);

            result.getFeatures().add((float) docLength);

//            float oov_cnt = 0;
//            for (String term : terms) {
//                if (collectionStats.getDF(term) < 100) {
//                    oov_cnt += 1;
//                }
//            }
//
//            float oov_pct = 0;
//            if (terms.size() != 0) {
//                oov_pct = oov_cnt / terms.size();
//            }
//            result.getFeatures().add(oov_pct);

            List<String> urls = extractor.extractURLs(result.getText());
            result.getFeatures().add((float) urls.size());

            if (urls.size() > 0) {
                result.getFeatures().add(1.0f);
            } else {
                result.getFeatures().add(0.0f);
            }

            List<String> hashtags = extractor.extractHashtags(result.getText());
            result.getFeatures().add((float) hashtags.size());

            if (hashtags.size() > 0) {
                result.getFeatures().add(1.0f);
            } else {
                result.getFeatures().add(0.0f);
            }

            List<String> mentions = extractor.extractMentionedScreennames(result.getText());
            result.getFeatures().add((float) mentions.size());

            if (mentions.size() > 0) {
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

            result.getFeatures().add((float) Doubles.max(tfValues));
        }

        try {
            results = rankRankLib(query, results, "RMTS");
        } catch (SecurityException e) {
            logger.warn("RankLib caught calling System.exit(int).");
        }
    }

    @SuppressWarnings("UnusedAssignment")
    public List<Document> rankRankLib(String query, List<Document> results, String runTag) {
        RankerFactory rFact = new RankerFactory();
        Ranker ranker = rFact.loadRanker("ltr-all.model");
        int[] features = ranker.getFeatures();
        List<DataPoint> rl = new ArrayList<>();

        String qid = query.replaceFirst("^MB0*", "");

        int i = 1;
        for (TResultWrapper hit : results) {
//            String rel = String.valueOf(qrels.getRel(qid, String.valueOf(hit.getId())));
//            DataPoint dp = hit.getDataPoint(rel, qid);
            DataPoint dp = hit.getDataPoint();
            rl.add(dp);
            if (i++ >= numRerank)
                break;
        }
        RankList l = new RankList(rl);

        LinearNormalizer nml = new LinearNormalizer();
        nml.normalize(l, features);

        double[] scores = new double[l.size()];
        for (int j = 0; j < l.size(); j++)
            scores[j] = ranker.eval(l.get(j));


        List<Document> finalResults = Lists.newArrayList();
        int[] idx = MergeSorter.sort(scores, false);
        for (int j = 0; j < Math.min(idx.length, numResults); j++) {
            int k = idx[j];
            String docno = l.get(k).getDescription().substring(2); // remove prefix "# "
//            String rel = qrels.getRelString(qid, docno);
            String rel = "0";

            float maxTF = l.get(k).getFeatureValue(21);
            if (maxTF < 5) {
                Document updatedResult = new Document(results.get(k));
                updatedResult.setRsv(scores[k]);
                finalResults.add(updatedResult);
            }

//            System.out.println(String.format("%s Q0 %s %d %." + (int) (6 + Math.ceil(Math.log10(numResults))) + "f %s # rel = %s, rt = %s, text = %s", qid, docno, (j + 1),
//                    scores[k], runTag, rel, results.get(j).getRetweeted_status_id(), results.get(j).getText().replaceAll("\\r?\\n", " --linebreak-- ")));
        }
        return finalResults;
    }
}
