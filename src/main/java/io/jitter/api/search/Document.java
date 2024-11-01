package io.jitter.api.search;

import io.jitter.core.document.DocVector;

public interface Document {
    String getId();

    void setId(String id);

    double getRsv();

    void setRsv(double rsv);

    String getText();

    void setText(String text);

    DocVector getDocVector();

    void setDocVector(DocVector docVector);
}
