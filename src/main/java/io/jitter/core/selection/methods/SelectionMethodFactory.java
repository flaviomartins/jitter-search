package io.jitter.core.selection.methods;

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
        } else if ("CRCSLOGEXP".equalsIgnoreCase(method)) {
            return new CRCSLOGEXP();
        } else if ("RankS".equalsIgnoreCase(method)) {
            return new RankS(true);
        }
        // default
        return new Votes();
    }

}
