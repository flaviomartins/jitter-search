package org.novasearch.jitter.core.selection.methods;

import org.novasearch.jitter.api.search.Document;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReDDE extends SelectionMethod {

    protected ReDDE() {
    }

    // TODO: implement ReDDE
    @Override
    public Map<String, Float> rank(List<Document> results) {
        HashMap<String, Float> map = new HashMap<>();
        for (Document result : results) {
            String screenName = result.getScreen_name();
            if (!map.containsKey(screenName)) {
                map.put(screenName, 1f);
            } else {
                float cur = map.get(screenName);
                map.put(screenName, cur + 1f);
            }
        }
        return map;
    }
}
