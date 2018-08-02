/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GPLv3
 * See license text in LICENSE.txt or at https://opensource.dbc.dk/licenses/gpl-3.0/
 */

package dk.dbc.rawrepo;

import dk.dbc.httpclient.HttpClient;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.jackson.JacksonFeature;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.client.Client;

/**
 * CDI Factory bean for RecordServerConnector singleton instance
 * <p>
 * Depends on the rawrepo record service baseurl being defined as
 * the value of either a system property or environment variable
 * named RAWREPO_RECORD_SERVICE_URL.
 * </p>
 */
@ApplicationScoped
public class RecordServiceConnectorFactory {
    @Inject
    @ConfigProperty(name = "RAWREPO_RECORD_SERVICE_URL")
    private String recordServiceBaseUrl;

    RecordServiceConnector recordServiceConnector;

    @PostConstruct
    public void initializeConnector() {
        final Client client = HttpClient.newClient(new ClientConfig()
                .register(new JacksonFeature()));
        recordServiceConnector = new RecordServiceConnector(client, recordServiceBaseUrl);
    }

    public RecordServiceConnector getInstance() {
        return recordServiceConnector;
    }

    @PreDestroy
    public void tearDownConnector() {
        recordServiceConnector.close();
    }
}
