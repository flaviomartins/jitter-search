package org.novasearch.jitter.core.selection.methods;

public class SelectionMethodFactory {

    public static SelectionMethod getMethod(String method) {
        if ("Votes".equalsIgnoreCase(method)) {
            return new Votes();
        } else if ("ReDDE".equalsIgnoreCase(method)) {
            return new ReDDE();
        } else if ("CRCSLIN".equalsIgnoreCase(method)) {
            return new CRCSLIN();
        } else if ("CRCSEXP".equalsIgnoreCase(method)) {
            return new CRCSEXP();
        } else if ("CRCSISR".equalsIgnoreCase(method)) {
            return new CRCSISR();
        } else if ("CRCSLOGISR".equalsIgnoreCase(method)) {
            return new CRCSLOGISR();
        } else if ("RankS".equalsIgnoreCase(method)) {
            return new RankS();
        } else if ("Taily".equalsIgnoreCase(method)) {
            return new Taily();
        }
        // default
        return new Votes();
    }

}
