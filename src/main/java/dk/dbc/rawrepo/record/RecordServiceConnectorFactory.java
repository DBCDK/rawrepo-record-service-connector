package dk.dbc.rawrepo.record;

import dk.dbc.httpclient.HttpClient;
import dk.dbc.rawrepo.record.RecordServiceConnector.TimingLogLevel;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.ws.rs.client.Client;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RecordServerConnector factory
 * <p>
 * Synopsis:
 * </p>
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
 * <p>
 * CDI case depends on the rawrepo record service baseurl being defined as
 * the value of either a system property or environment variable
 * named RAWREPO_RECORD_SERVICE_URL. RAWREPO_RECORD_SERVICE_TIMING_LOG_LEVEL
 * should be one of TRACE, DEBUG, INFO(default), WARN or ERROR, for setting
 * log level
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

    public static RecordServiceConnector create(String recordServiceBaseUrl, TimingLogLevel level) {
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
    private TimingLogLevel level;

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
