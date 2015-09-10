package io.jitter.core.utils;

public class Epochs {

    public static long[] parseEpochRange(String epochRange) {
        long[] epochs = new long[]{0L, Long.MAX_VALUE};
        String[] split = epochRange.split("[: ]");
        try {
            if (split.length == 1) {
                epochs[1] = Long.parseLong(split[0]);
            } else {
                epochs[0] = Long.parseLong(split[0]);
                epochs[1] = Long.parseLong(split[1]);
            }
        } catch (Exception e) {
            // pass
        }
        return epochs;
    }
}
