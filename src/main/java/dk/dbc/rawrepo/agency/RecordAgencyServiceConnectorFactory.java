package dk.dbc.rawrepo.agency;

import dk.dbc.httpclient.HttpClient;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.ws.rs.client.Client;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordAgencyServiceConnectorFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordAgencyServiceConnectorFactory.class);

    public static RecordAgencyServiceConnector create(String recordAgencyServiceBaseUrl) {
        final Client client = HttpClient.newClient(new ClientConfig()
                .register(new JacksonFeature()));
        LOGGER.info("Creating RecordAgencyServiceConnector for: {}", recordAgencyServiceBaseUrl);
        return new RecordAgencyServiceConnector(client, recordAgencyServiceBaseUrl);
    }

    public static RecordAgencyServiceConnector create(String recordAgencyServiceBaseUrl, RecordAgencyServiceConnector.TimingLogLevel level) {
        final Client client = HttpClient.newClient(new ClientConfig()
                .register(new JacksonFeature()));
        LOGGER.info("Creating RecordAgencyServiceConnector for: {}", recordAgencyServiceBaseUrl);
        return new RecordAgencyServiceConnector(client, recordAgencyServiceBaseUrl, level);
    }

    @Inject
    @ConfigProperty(name = "RAWREPO_RECORD_SERVICE_URL")
    private String recordAgencyServiceBaseUrl;

    @Inject
    @ConfigProperty(name = "RAWREPO_RECORD_SERVICE_TIMING_LOG_LEVEL", defaultValue = "INFO")
    private RecordAgencyServiceConnector.TimingLogLevel level;

    RecordAgencyServiceConnector recordAgencyServiceConnector;

    @PostConstruct
    public void initializeConnector() {
        recordAgencyServiceConnector = RecordAgencyServiceConnectorFactory.create(recordAgencyServiceBaseUrl, level);
    }

    @Produces
    public RecordAgencyServiceConnector getInstance() {
        return recordAgencyServiceConnector;
    }

    @PreDestroy
    public void tearDownConnector() {
        recordAgencyServiceConnector.close();
    }
}
