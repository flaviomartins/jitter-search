package io.jitter.core.wikipedia;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.io.FilenameUtils;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class PetscanCsvCategoryMapper {

    private Map<String, Set<String>> map = new HashMap<>();

    public PetscanCsvCategoryMapper(File file) throws IOException {
        Preconditions.checkNotNull(file);

        if (!file.isDirectory()) {
            throw new IOException("Expecting " + file + " to be a directory!");
        }

        final File[] files = file.listFiles(new FileFilter() {
            public boolean accept(File path) {
                return path.getName().endsWith(".csv") ? true : false;
            }
        });

        if (files.length == 0) {
            throw new IOException(file + " does not contain any .csv files!");
        }

        for (File csv : files) {
            String topicName = FilenameUtils.getBaseName(csv.getName());
            Reader in = new FileReader(csv);
            Iterable<CSVRecord> records = CSVFormat.DEFAULT.withFirstRecordAsHeader().withEscape('\\').parse(in);
            for (CSVRecord record : records) {
                String title = record.get("title").replace("Category:", "");
                if (!map.containsKey(title)) {
                    map.put(title, Sets.newHashSet(topicName));
                } else {
                    map.get(title).add(topicName);
                }
            }
            in.close();
        }
    }

    public Map<String, Set<String>> getMap() {
        return map;
    }
}
