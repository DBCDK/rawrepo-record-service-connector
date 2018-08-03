/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GPLv3
 * See license text in LICENSE.txt or at https://opensource.dbc.dk/licenses/gpl-3.0/
 */

package dk.dbc.rawrepo;

import dk.dbc.httpclient.HttpClient;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.ws.rs.client.Client;

/**
 * RecordServerConnector factory
 * <p>
 * Synopsis:
 * <pre>
 *    // New instance
 *    RecordServiceConnector rsc = RecordServiceConnector.create("http://record-service");
 *
 *    // Singleton instance in CDI enabled environment
 *    {@literal @}Inject
 *    RecordServiceConnectorFactory factory;
 *    ...
 *    RecordServiceConnector rsc = factory.getInstance();
 *
 *    // or simply
 *    {@literal @}Inject
 *    RecordServiceConnector rsc;
 * </pre>
 * </p>
 * <p>
 * CDI case depends on the rawrepo record service baseurl being defined as
 * the value of either a system property or environment variable
 * named RAWREPO_RECORD_SERVICE_URL.
 * </p>
 */
@ApplicationScoped
public class RecordServiceConnectorFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordServiceConnectorFactory.class);

    public static RecordServiceConnector create(String recordServiceBaseUrl) {
        final Client client = HttpClient.newClient(new ClientConfig()
                .register(new JacksonFeature()));
        LOGGER.info("Creating RecordServiceConnector for: {}", recordServiceBaseUrl);
        return new RecordServiceConnector(client, recordServiceBaseUrl);
    }

    @Inject
    @ConfigProperty(name = "RAWREPO_RECORD_SERVICE_URL")
    private String recordServiceBaseUrl;

    RecordServiceConnector recordServiceConnector;

    @PostConstruct
    public void initializeConnector() {
        recordServiceConnector = RecordServiceConnectorFactory.create(recordServiceBaseUrl);
    }

    @Produces
    public RecordServiceConnector getInstance() {
        return recordServiceConnector;
    }

    @PreDestroy
    public void tearDownConnector() {
        recordServiceConnector.close();
    }
}
