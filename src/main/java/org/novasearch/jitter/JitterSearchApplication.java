package org.novasearch.jitter;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.novasearch.jitter.core.ReputationReader;
import org.novasearch.jitter.resources.ReputationResource;
import org.novasearch.jitter.resources.SearchResource;
import org.novasearch.jitter.resources.TopTermsResource;

import java.io.File;
import java.io.IOException;

public class JitterSearchApplication extends Application<JitterSearchConfiguration> {
    public static void main(String[] args) throws Exception {
        new JitterSearchApplication().run(args);
    }

    @Override
    public String getName() {
        return "jitter-responses";
    }

    @Override
    public void initialize(Bootstrap<JitterSearchConfiguration> bootstrap) {
        // nothing to do yet
    }

    @Override
    public void run(JitterSearchConfiguration configuration,
                    Environment environment) throws IOException {
        final ReputationReader reputationReader = new ReputationReader(new File(configuration.getReputationFile()));

        final SearchResource searchResource = new SearchResource(new File(configuration.getIndex()), reputationReader);
        environment.jersey().register(searchResource);

        final ReputationResource reputationResource = new ReputationResource(reputationReader);
        environment.jersey().register(reputationResource);

        final TopTermsResource topTermsResource = new TopTermsResource(new File(configuration.getIndex()));
        environment.jersey().register(topTermsResource);
    }
}
