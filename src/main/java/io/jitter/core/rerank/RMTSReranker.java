package io.jitter.core.rerank;

import cc.twittertools.index.IndexStatuses;
import ciir.umass.edu.features.LinearNormalizer;
import ciir.umass.edu.learning.DataPoint;
import ciir.umass.edu.learning.RankList;
import ciir.umass.edu.learning.Ranker;
import ciir.umass.edu.learning.RankerFactory;
import ciir.umass.edu.utilities.MergeSorter;
import com.google.common.collect.Lists;
import com.twitter.Extractor;
import io.jitter.api.collectionstatistics.CollectionStats;
import io.jitter.api.search.Document;
import io.jitter.core.analysis.StopperTweetAnalyzer;
import io.jitter.core.document.DocVector;
import io.jitter.core.features.BM25Feature;
import io.jitter.core.probabilitydistributions.KDE;
import io.jitter.core.probabilitydistributions.LocalExponentialDistribution;
import io.jitter.core.utils.TimeUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class RMTSReranker {
    private static final Logger logger = LoggerFactory.getLogger(RMTSReranker.class);

    private static final double DAY = 60.0 * 60.0 * 24.0;

    private final Ranker ranker;

    private final Analyzer analyzer;
    private final QueryParser QUERY_PARSER;

    public RMTSReranker(String rankerModel) {
        RankerFactory rFact = new RankerFactory();
        ranker = rFact.loadRankerFromFile(rankerModel);
        
        analyzer = new StopperTweetAnalyzer(Version.LUCENE_43, CharArraySet.EMPTY_SET, true, false, true);
        QUERY_PARSER = new QueryParser(IndexStatuses.StatusField.TEXT.name, analyzer);
    }

    public List<Document> score(String query, double queryEpoch, List<Document> results, CollectionStats collectionStats, int numResults, int numRerank) throws ParseException {
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

        Query q = QUERY_PARSER.parse(query.replaceAll(",", ""));
        Set<Term> queryTerms = new TreeSet<>();
        q.extractTerms(queryTerms);
        Set<String> qTerms = new HashSet<>();
        for (Term term : queryTerms) {
            String text = term.text();
            if (text.isEmpty())
                continue;
            qTerms.add(text);
        }

        for (Document result : results) {
            DocVector docVector = result.getDocVector();

            double averageDocumentLength = 28;
            double docLength = 0;
            for (Integer i : docVector.vector.values()) {
                docLength += i;
            }

            double idf = 0;
            double bm25 = 0;
            double coord = 0;
            double tfMax = 0;

            for (Map.Entry<String, Integer> tf : docVector.vector.entrySet()) {
                String term = tf.getKey();
                if (qTerms.contains(term)) {
                    double tfValue = tf.getValue();
                    if (tfValue > 0) {
                        idf += collectionStats.getIDF(term);
                        bm25 += bm25Feature.value(tfValue, docLength, averageDocumentLength, collectionStats.getDF(term), collectionStats.getCollectionSize());
                        coord += 1;
                        tfMax = Math.max(tfMax, tfValue);
                    }
                }
            }

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

            result.getFeatures().add((float) tfMax);
        }

        try {
            results = rankRankLib(query, results, "RMTS", numResults, numRerank);
        } catch (SecurityException e) {
            logger.warn("RankLib caught calling System.exit(int).");
        }
        return results;
    }

    @SuppressWarnings("UnusedAssignment")
    public List<Document> rankRankLib(String query, List<Document> results, String runTag, int numResults, int numRerank) {
        int[] features = ranker.getFeatures();
        List<DataPoint> rl = new ArrayList<>();

        String qid = query.replaceFirst("^MB0*", "");

        int i = 1;
        for (Document hit : results) {
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
            String docno = l.get(k).getDescription();
//            String rel = qrels.getRelString(qid, docno);
            String rel = "0";

            float maxTF = l.get(k).getFeatureValue(21);
            if (maxTF < 3) {
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
