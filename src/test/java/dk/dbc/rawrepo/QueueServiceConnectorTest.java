package dk.dbc.rawrepo;

import com.github.tomakehurst.wiremock.WireMockServer;
import dk.dbc.httpclient.HttpClient;
import dk.dbc.rawrepo.dto.EnqueueAgencyResponseDTO;
import dk.dbc.rawrepo.dto.EnqueueResultCollectionDTO;
import dk.dbc.rawrepo.dto.EnqueueResultDTO;
import dk.dbc.rawrepo.dto.QueueProviderCollectionDTO;
import dk.dbc.rawrepo.dto.QueueRuleCollectionDTO;
import dk.dbc.rawrepo.dto.QueueRuleDTO;
import dk.dbc.rawrepo.dto.QueueWorkerCollectionDTO;
import dk.dbc.rawrepo.queue.QueueServiceConnector;
import dk.dbc.rawrepo.queue.QueueServiceConnectorException;
import jakarta.ws.rs.client.Client;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.configureFor;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class QueueServiceConnectorTest {
    private static WireMockServer wireMockServer;
    private static String wireMockHost;

    final static Client CLIENT = HttpClient.newClient(new ClientConfig()
            .register(new JacksonFeature()));
    static QueueServiceConnector connector;

    @BeforeAll
    static void startWireMockServer() {
        wireMockServer = new WireMockServer(options().dynamicPort()
                .dynamicHttpsPort());
        wireMockServer.start();
        wireMockHost = "http://localhost:" + wireMockServer.port();
        configureFor("localhost", wireMockServer.port());
    }

    @BeforeAll
    static void setConnector() {
        connector = new QueueServiceConnector(CLIENT, wireMockHost, QueueServiceConnector.TimingLogLevel.INFO);
    }

    @AfterAll
    static void stopWireMockServer() {
        wireMockServer.stop();
    }

    @Test
    void getQueueRulesTest() throws QueueServiceConnectorException {
        QueueRuleCollectionDTO actual = connector.getQueueRules();
        assertThat("queue rule count", actual.getQueueRules().size(), is(33));

        QueueRuleDTO queueRuleDTO = actual.getQueueRules().get(0);
        assertThat("queue rule 0 provider", queueRuleDTO.getProvider(), is("agency-delete"));
        assertThat("queue rule 0 worker", queueRuleDTO.getWorker(), is("broend-sync"));
        assertThat("queue rule 0 changed", queueRuleDTO.getChanged(), is('A'));
        assertThat("queue rule 0 leaf", queueRuleDTO.getLeaf(), is('Y'));
        assertThat("queue rule 0 description", queueRuleDTO.getDescription(), is("Alle Bind/Enkeltstående poster som er afhængige af den rørte post incl den rørte post"));

        queueRuleDTO = actual.getQueueRules().get(10);
        assertThat("queue rule 10 provider", queueRuleDTO.getProvider(), is("dataio-update"));
        assertThat("queue rule 10 worker", queueRuleDTO.getWorker(), is("oai-set-matcher"));
        assertThat("queue rule 10 changed", queueRuleDTO.getChanged(), is('A'));
        assertThat("queue rule 10 leaf", queueRuleDTO.getLeaf(), is('Y'));
        assertThat("queue rule 10 description", queueRuleDTO.getDescription(), is("Alle Bind/Enkeltstående poster som er afhængige af den rørte post incl den rørte post"));

        queueRuleDTO = actual.getQueueRules().get(30);
        assertThat("queue rule 30 provider", queueRuleDTO.getProvider(), is("solr-sync-bulk"));
        assertThat("queue rule 30 worker", queueRuleDTO.getWorker(), is("solr-sync-bulk"));
        assertThat("queue rule 30 changed", queueRuleDTO.getChanged(), is('Y'));
        assertThat("queue rule 30 leaf", queueRuleDTO.getLeaf(), is('N'));
        assertThat("queue rule 30 description", queueRuleDTO.getDescription(), is("Den rørte post, hvis det er en Hoved/Sektionsport"));
    }

    @Test
    void getQueueProviders() throws QueueServiceConnectorException {
        final List<String> expected = Arrays.asList("agency-delete",
                "agency-maintain",
                "bulk-broend",
                "dataio-bulk",
                "dataio-ph-holding-update",
                "dataio-update",
                "dataio-update-well3.5",
                "fbs-ph-update",
                "fbs-update",
                "ims",
                "ims-bulk",
                "opencataloging-update",
                "solr-sync-bulk",
                "update-rawrepo-solr-sync");

        QueueProviderCollectionDTO actual = connector.getQueueProviders();

        assertThat("provider list", actual.getProviders(), is(expected));
    }

    @Test
    void getQueueWorkers() throws QueueServiceConnectorException {
        final List<String> expected = Arrays.asList("basis-decentral",
                "broend-sync",
                "danbib-ph-libv3",
                "dataio-bulk-sync",
                "dataio-socl-sync-bulk",
                "ims-bulk-sync",
                "ims-sync",
                "oai-set-matcher",
                "socl-sync",
                "solr-sync-basis",
                "solr-sync-bulk");

        QueueWorkerCollectionDTO actual = connector.getQueueWorkers();

        assertThat("worker list", actual.getWorkers(), is(expected));
    }

    @Test
    void enqueueRecordNoParams() throws QueueServiceConnectorException {
        final List<EnqueueResultDTO> enqueueResults = new ArrayList<>();
        enqueueResults.add(new EnqueueResultDTO("50129691", 870970, "broend-sync", true));
        enqueueResults.add(new EnqueueResultDTO("50129691", 870970, "danbib-ph-libv3", true));
        enqueueResults.add(new EnqueueResultDTO("50129691", 870970, "socl-sync", true));
        final EnqueueResultCollectionDTO expected = new EnqueueResultCollectionDTO();
        expected.setEnqueueResults(enqueueResults);

        final EnqueueResultCollectionDTO actual = connector.enqueueRecord(870970, "50129691", "fbs-ph-update");

        assertThat("enqueue record result matches", actual, is(expected));
    }

    @Test
    void enqueueRecordParams() throws QueueServiceConnectorException {
        final EnqueueResultCollectionDTO expected = new EnqueueResultCollectionDTO();
        final List<EnqueueResultDTO> enqueueResults = new ArrayList<>();
        enqueueResults.add(new EnqueueResultDTO("50129691", 870970, "broend-sync", false));
        enqueueResults.add(new EnqueueResultDTO("50129691", 870970, "danbib-ph-libv3", false));
        expected.setEnqueueResults(enqueueResults);

        final QueueServiceConnector.EnqueueParams params = new QueueServiceConnector.EnqueueParams()
                .withChanged(false)
                .withLeaf(true);
        final EnqueueResultCollectionDTO actual = connector.enqueueRecord(870970, "50129691", "fbs-ph-update", params);

        assertThat("enqueue record result matches", actual, is(expected));
    }

    @Test
    void enqueueAgencyNoParams() throws QueueServiceConnectorException {
        EnqueueAgencyResponseDTO actual = connector.enqueueAgency(870970, "socl-sync");
        assertThat("get count", actual.getCount(), is(10));
    }

    @Test
    void enqueueAgencyAsAgency() throws QueueServiceConnectorException {
        QueueServiceConnector.EnqueueParams params = new QueueServiceConnector.EnqueueParams()
                .withEnqueueAs(191919);

        EnqueueAgencyResponseDTO actual = connector.enqueueAgency(870971, "socl-sync");
        assertThat("get count", actual.getCount(), is(1));
    }

}
