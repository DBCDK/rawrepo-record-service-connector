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

@ApplicationScoped
public class RecordDumpServiceConnectorFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordDumpServiceConnectorFactory.class);

    public static RecordDumpServiceConnector create(String recordServiceBaseUrl) {
        final Client client = HttpClient.newClient(new ClientConfig()
                .register(new JacksonFeature()));
        LOGGER.info("Creating RecordDumpServiceConnector for: {}", recordServiceBaseUrl);
        return new RecordDumpServiceConnector(client, recordServiceBaseUrl);
    }

    public static RecordDumpServiceConnector create(String recordServiceBaseUrl, RecordDumpServiceConnector.TimingLogLevel level) {
        final Client client = HttpClient.newClient(new ClientConfig()
                .register(new JacksonFeature()));
        LOGGER.info("Creating RecordDumpServiceConnector for: {}", recordServiceBaseUrl);
        return new RecordDumpServiceConnector(client, recordServiceBaseUrl, level);
    }

    @Inject
    @ConfigProperty(name = "RAWREPO_RECORD_SERVICE_URL")
    private String recordServiceBaseUrl;

    @Inject
    @ConfigProperty(name = "RAWREPO_RECORD_SERVICE_TIMING_LOG_LEVEL", defaultValue = "INFO")
    private RecordDumpServiceConnector.TimingLogLevel level;

    RecordDumpServiceConnector recordDumpServiceConnector;

    @PostConstruct
    public void initializeConnector() {
        recordDumpServiceConnector = RecordDumpServiceConnectorFactory.create(recordServiceBaseUrl, level);
    }

    @Produces
    public RecordDumpServiceConnector getInstance() {
        return recordDumpServiceConnector;
    }

    @PreDestroy
    public void tearDownConnector() {
        recordDumpServiceConnector.close();
    }
}
