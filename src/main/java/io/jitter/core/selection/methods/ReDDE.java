package io.jitter.core.selection.methods;

import io.jitter.api.search.Document;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReDDE extends SelectionMethod {

    ReDDE() {
    }

    // TODO: implement ReDDE
    @Override
    public Map<String, Double> rank(List<Document> results) {
        HashMap<String, Double> map = new HashMap<>();
        for (Document result : results) {
            String screenName = result.getScreen_name();
            if (!map.containsKey(screenName)) {
                map.put(screenName, 1d);
            } else {
                double cur = map.get(screenName);
                map.put(screenName, cur + 1d);
            }
        }
        return map;
    }
}