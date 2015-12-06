package io.jitter.resources;

import cc.twittertools.index.TweetAnalyzer;
import ciir.umass.edu.features.LinearNormalizer;
import ciir.umass.edu.learning.DataPoint;
import ciir.umass.edu.learning.RankList;
import ciir.umass.edu.learning.Ranker;
import ciir.umass.edu.learning.RankerFactory;
import ciir.umass.edu.utilities.MergeSorter;
import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.twitter.Extractor;
import io.dropwizard.jersey.params.BooleanParam;
import io.dropwizard.jersey.params.IntParam;
import io.jitter.api.ResponseHeader;
import io.jitter.api.collectionstatistics.CollectionStats;
import io.jitter.api.search.Document;
import io.jitter.api.search.SelectDocumentsResponse;
import io.jitter.api.search.SelectSearchResponse;
import io.jitter.core.features.BM25Feature;
import io.jitter.core.probabilitydistributions.KDE;
import io.jitter.core.probabilitydistributions.LocalExponentialDistribution;
import io.jitter.core.rerank.KDEReranker;
import io.jitter.core.rerank.RecencyReranker;
import io.jitter.core.search.SearchManager;
import io.jitter.core.selection.SelectionManager;
import io.jitter.core.selection.methods.SelectionMethod;
import io.jitter.core.selection.methods.SelectionMethodFactory;
import io.jitter.core.twittertools.api.TResultWrapper;
import io.jitter.core.utils.AnalyzerUtils;
import io.jitter.core.utils.Epochs;
import io.jitter.core.utils.TimeUtils;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

@Path("/rmts")
@Produces(MediaType.APPLICATION_JSON + "; charset=utf-8")
public class RMTSResource {
    private static final Logger logger = LoggerFactory.getLogger(RMTSResource.class);

    public static final double DAY = 60.0 * 60.0 * 24.0;

    private final AtomicLong counter;
    private final SearchManager searchManager;
    private final SelectionManager selectionManager;

    public RMTSResource(SearchManager searchManager, SelectionManager selectionManager) throws IOException {
        Preconditions.checkNotNull(searchManager);
        Preconditions.checkNotNull(selectionManager);

        counter = new AtomicLong();
        this.searchManager = searchManager;
        this.selectionManager = selectionManager;
    }

    @GET
    @Timed
    public SelectSearchResponse search(@QueryParam("q") Optional<String> q,
                                       @QueryParam("fq") Optional<String> fq,
                                       @QueryParam("limit") @DefaultValue("1000") IntParam limit,
                                       @QueryParam("retweets") @DefaultValue("false") BooleanParam retweets,
                                       @QueryParam("maxId") Optional<Long> maxId,
                                       @QueryParam("epoch") Optional<String> epoch,
                                       @QueryParam("sLimit") @DefaultValue("50") IntParam sLimit,
                                       @QueryParam("sRetweets") @DefaultValue("true") BooleanParam sRetweets,
                                       @QueryParam("method") @DefaultValue("crcsexp") String method,
                                       @QueryParam("maxCol") @DefaultValue("3") IntParam maxCol,
                                       @QueryParam("minRanks") @DefaultValue("1e-5") Double minRanks,
                                       @QueryParam("normalize") @DefaultValue("true") BooleanParam normalize,
                                       @QueryParam("topic") Optional<String> topic,
                                       @QueryParam("fbDocs") @DefaultValue("50") IntParam fbDocs,
                                       @QueryParam("fbTerms") @DefaultValue("20") IntParam fbTerms,
                                       @QueryParam("fbWeight") @DefaultValue("0.5") Double fbWeight,
                                       @QueryParam("fbTopics") @DefaultValue("3") IntParam fbTopics,
                                       @QueryParam("numRerank") @DefaultValue("1000") IntParam numRerank,
                                       @Context UriInfo uriInfo)
            throws IOException, ParseException {
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();

        String query = URLDecoder.decode(q.or(""), "UTF-8");
        List<Document> selectResults = null;
        List<Document> results = null;

        long[] epochs = new long[2];
        if (epoch.isPresent()) {
            Epochs.parseEpochRange(epoch.get());
        }

        long startTime = System.currentTimeMillis();

        if (q.isPresent()) {
            if (maxId.isPresent()) {
                selectResults = selectionManager.search(query, sLimit.get(), !sRetweets.get(), maxId.get());
            } else if (epoch.isPresent()) {
                selectResults = selectionManager.search(query, sLimit.get(), !sRetweets.get(), epochs[0], epochs[1]);
            } else {
                selectResults = selectionManager.search(query, sLimit.get(), !sRetweets.get());
            }
        }

        SelectionMethod selectionMethod = SelectionMethodFactory.getMethod(method);
        String methodName = selectionMethod.getClass().getSimpleName();

        Map<String, Double> rankedSources = selectionManager.getRanked(selectionMethod, selectResults, normalize.get());
        Map<String, Double> sources = selectionManager.limit(selectionMethod, rankedSources, maxCol.get(), minRanks);

        Map<String, Double> rankedTopics = selectionManager.getRankedTopics(selectionMethod, selectResults, normalize.get());
        Map<String, Double> topics = selectionManager.limit(selectionMethod, rankedTopics, maxCol.get(), minRanks);

        if (topics.size() > 0) {

        }

        // get the query epoch
        double currentEpoch = System.currentTimeMillis() / 1000L;
        double queryEpoch = epoch.isPresent() ? epochs[1] : currentEpoch;

        if (q.isPresent()) {
            if (maxId.isPresent()) {
                results = searchManager.search(query, numRerank.get(), !retweets.get(), maxId.get());
            } else if (epoch.isPresent()) {
                results = searchManager.search(query, numRerank.get(), !retweets.get(), epochs[0], epochs[1]);
            } else {
                results = searchManager.search(query, numRerank.get(), !retweets.get());
            }
        }

        score(query, queryEpoch, results);
        results = rankRankLib(query, results, numRerank.get(), limit.get(), "RMTS");

        long endTime = System.currentTimeMillis();

        int totalHits = results != null ? results.size() : 0;

        logger.info(String.format(Locale.ENGLISH, "%4dms %4dhits %s", (endTime - startTime), totalHits, query));

        ResponseHeader responseHeader = new ResponseHeader(counter.incrementAndGet(), 0, (endTime - startTime), params);
        SelectDocumentsResponse documentsResponse = new SelectDocumentsResponse(sources, topics, methodName, totalHits, 0, selectResults, results);
        return new SelectSearchResponse(responseHeader, documentsResponse);
    }

    protected void score(String query, double queryEpoch, List<Document> results) {
        for (Document result : results) {
            result.getFeatures().add((float) result.getRsv());
        }
//        if (filterRT) {
//            RetweetFilter rtFilter = new RetweetFilter(results);
//            results = rtFilter.getFiltered();
//        }
//
//        if (filterLang) {
//            langFilter.setResults(results);
//            results = langFilter.getFiltered();
//        }

//        List<Document> results = tResults;


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

        KDEReranker kdeReranker = new KDEReranker(results, queryEpoch, method, KDEReranker.WEIGHT.UNIFORM, 1.0);
        results = kdeReranker.getReranked();
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

        CollectionStats collectionStats = searchManager.getCollectionStats();
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
        }
    }

    public List<Document> rankRankLib(String query, List<Document> results, int numRerank, int numResults, String runTag) {
        RankerFactory rFact = new RankerFactory();
        Ranker ranker = rFact.loadRanker("ltr-all.model");
        int[] features = ranker.getFeatures();
        List<DataPoint> rl = new ArrayList<DataPoint>();

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

            Document updatedResult = new Document(results.get(j));
            updatedResult.setRsv(scores[k]);
            finalResults.add(updatedResult);

//            System.out.println(String.format("%s Q0 %s %d %." + (int) (6 + Math.ceil(Math.log10(numResults))) + "f %s # rel = %s, rt = %s, text = %s", qid, docno, (j + 1),
//                    scores[k], runTag, rel, results.get(j).getRetweeted_status_id(), results.get(j).getText().replaceAll("\\r?\\n", " --linebreak-- ")));
        }
        return finalResults;
    }
}
