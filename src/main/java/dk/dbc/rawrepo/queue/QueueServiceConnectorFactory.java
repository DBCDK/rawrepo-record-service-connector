/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GPLv3
 * See license text in LICENSE.txt or at https://opensource.dbc.dk/licenses/gpl-3.0/
 */

package dk.dbc.rawrepo.queue;

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
public class QueueServiceConnectorFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueueServiceConnectorFactory.class);

    public static QueueServiceConnector create(String recordServiceBaseUrl) {
        final Client client = HttpClient.newClient(new ClientConfig()
                .register(new JacksonFeature()));
        LOGGER.info("Creating QueueServiceConnector for: {}", recordServiceBaseUrl);
        return new QueueServiceConnector(client, recordServiceBaseUrl);
    }

    public static QueueServiceConnector create(String recordServiceBaseUrl, QueueServiceConnector.TimingLogLevel level) {
        final Client client = HttpClient.newClient(new ClientConfig()
                .register(new JacksonFeature()));
        LOGGER.info("Creating QueueServiceConnector for: {}", recordServiceBaseUrl);
        return new QueueServiceConnector(client, recordServiceBaseUrl, level);
    }

    @Inject
    @ConfigProperty(name = "RAWREPO_RECORD_SERVICE_URL")
    private String recordServiceBaseUrl;

    @Inject
    @ConfigProperty(name = "RAWREPO_RECORD_SERVICE_TIMING_LOG_LEVEL", defaultValue = "INFO")
    private QueueServiceConnector.TimingLogLevel level;

    QueueServiceConnector queueServiceConnector;

    @PostConstruct
    public void initializeConnector() {
        queueServiceConnector = QueueServiceConnectorFactory.create(recordServiceBaseUrl, level);
    }

    @Produces
    public QueueServiceConnector getInstance() {
        return queueServiceConnector;
    }

    @PreDestroy
    public void tearDownConnector() {
        queueServiceConnector.close();
    }
}
