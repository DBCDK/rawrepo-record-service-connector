package dk.dbc.rawrepo;

import dk.dbc.httpclient.HttpClient;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.ws.rs.client.Client;

public class RecordAgencyServiceConnectorFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordAgencyServiceConnectorFactory.class);

    public static RecordServiceConnector create(String recordServiceBaseUrl) {
        final Client client = HttpClient.newClient(new ClientConfig()
                .register(new JacksonFeature()));
        LOGGER.info("Creating RecordServiceConnector for: {}", recordServiceBaseUrl);
        return new RecordServiceConnector(client, recordServiceBaseUrl);
    }

    public static RecordServiceConnector create(String recordServiceBaseUrl, RecordServiceConnector.TimingLogLevel level) {
        final Client client = HttpClient.newClient(new ClientConfig()
                .register(new JacksonFeature()));
        LOGGER.info("Creating RecordServiceConnector for: {}", recordServiceBaseUrl);
        return new RecordServiceConnector(client, recordServiceBaseUrl, level);
    }

    @Inject
    @ConfigProperty(name = "RAWREPO_RECORD_SERVICE_URL")
    private String recordServiceBaseUrl;

    @Inject
    @ConfigProperty(name = "RAWREPO_RECORD_SERVICE_TIMING_LOG_LEVEL", defaultValue = "INFO")
    private RecordServiceConnector.TimingLogLevel level;

    RecordServiceConnector recordServiceConnector;

    @PostConstruct
    public void initializeConnector() {
        recordServiceConnector = RecordServiceConnectorFactory.create(recordServiceBaseUrl, level);
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
