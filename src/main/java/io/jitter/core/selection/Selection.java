package io.jitter.core.selection;

import java.util.Map;

public interface Selection {

    SelectionTopDocuments getResults();

    Map<String, Double> getCollections();

}
