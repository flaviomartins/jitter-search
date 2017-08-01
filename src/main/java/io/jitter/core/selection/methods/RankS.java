package io.jitter.core.selection.methods;

import io.jitter.api.search.ShardedDocument;
import io.jitter.core.shards.ShardStats;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class RankS extends SelectionMethod {
    // Recommended range for the exponent base is [10, 100]
    private final int B;
    private final boolean useScores;

    protected RankS(int B, boolean useScores) {
        this.B = B;
        this.useScores = useScores;
    }

    protected RankS(boolean useScores) {
        this(50, useScores);
    }

    @Override
    public Map<String, Double> rank(List<ShardedDocument> results, ShardStats csiStats) {
        Map<String, Double> counts = getCounts(results);

        double minRsv = 0;
        if (useScores) {
            minRsv = getMinRsv(results);
        }

        HashMap<String, Double> map = new HashMap<>();
        int j = 1;
        int step = 1;
        String[] topShards = null;
        for (ShardedDocument result : results) {
            if (j == 1) {
                topShards = result.getShardIds();
            }

            double r = getStepFactor(step);
            if (useScores) {
                if (minRsv < 0) {
                    r *= (result.getRsv() + Math.abs(minRsv));
                } else {
                    r *= result.getRsv();
                }
            }

            String[] shardIds = result.getShardIds();
            for (String shardId : shardIds) {
                if (!map.containsKey(shardId)) {
                    map.put(shardId, r);
                } else {
                    double cur = map.get(shardId);
                    map.put(shardId, cur + r);
                }
            }

            if (j > 1) {
                step++;
            }
            j++;
        }

        for (String topShard : topShards) {
            if (counts.containsKey(topShard)) {
                if (counts.get(topShard) == 1) {
                    map.remove(topShard);
                }
            }
        }

        return map;
    }

    private double getMinRsv(List<ShardedDocument> results) {
        double minRsv = Double.MAX_VALUE;
        for (ShardedDocument result : results) {
            if (result.getRsv() < minRsv) {
                minRsv = result.getRsv();
            }
        }
        return minRsv;
    }

    private double getStepFactor(int step) {
        return Math.pow(B, -step);
    }

    @Override
    public Map<String, Double> normalize(Map<String, Double> rank, ShardStats csiStats, ShardStats shardStats) {
        HashMap<String, Double> map = new HashMap<>();
        for (Map.Entry<String, Double> shardScoreEntry : rank.entrySet()) {
            String shardIdLower = shardScoreEntry.getKey().toLowerCase(Locale.ROOT);
            if (shardStats.getSizes().containsKey(shardIdLower)) {
                double c_i = shardStats.getSizes().get(shardIdLower);
                double s_i = csiStats.getSizes().get(shardIdLower);
                double norm = c_i / s_i;
                double origScore = shardScoreEntry.getValue();
                double newScore = norm * origScore;
                map.put(shardScoreEntry.getKey(), newScore);
            }
        }
        return map;
    }
}
