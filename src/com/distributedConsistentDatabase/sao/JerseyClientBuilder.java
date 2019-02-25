package com.distributedConsistentDatabase.sao;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;

/**
 * Builder class to generate the JerseyClient object.
 * @author abshukla
 *
 */
public class JerseyClientBuilder {

    /**
     * Creates a client object which is thread-safe by default.
     * @return : Jersey client.
     */
    public static Client getClient() {
        final ClientConfig clientConfig = new DefaultClientConfig();
        clientConfig.getFeatures().put(
                JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
        return Client.create(clientConfig);
    }
}
