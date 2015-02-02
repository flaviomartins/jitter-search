package org.novasearch.jitter.core;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ReputationReader {
    private static final Logger logger = Logger.getLogger(ReputationReader.class);

    private final Map<String, Double> entityReputation;

    public ReputationReader(File file) throws IOException {
        JsonFactory factory = new JsonFactory();
        ObjectMapper mapper = new ObjectMapper(factory);
        TypeReference<HashMap<String, Double>> typeRef = new TypeReference<HashMap<String, Double>>() {
        };
        entityReputation = mapper.readValue(file, typeRef);
    }

    public double getReputation(String entity) {
        String key = Joiner.on("##").join(entity.split(" ")).toLowerCase();
        if (!entityReputation.containsKey(key))
            return -1;
        return entityReputation.get(key);
    }

    public Map<String,Double> getEntitiesReputation(String text) {
        String normText = " " + text.replaceAll("[^a-zA-Z ]", " ").toLowerCase() + " ";

        Map<String,Double> found = Maps.newHashMap();
        for (String entity : entityReputation.keySet()) {
            String normEntity = " " + entity.replace("##", " ").toLowerCase() + " ";
            if (normText.contains(normEntity)) {
                double reputation = getReputation(entity);
                if (reputation != -1)
                    found.put(normEntity, reputation);
            }
        }
        return found;
    }

}
