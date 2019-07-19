/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GPLv3
 * See license text in LICENSE.txt or at https://opensource.dbc.dk/licenses/gpl-3.0/
 */

package dk.dbc.rawrepo;

import com.github.tomakehurst.wiremock.WireMockServer;
import dk.dbc.httpclient.HttpClient;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.ws.rs.client.Client;
import java.util.HashMap;

import static com.github.tomakehurst.wiremock.client.WireMock.configureFor;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContaining;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RecordServiceConnectorTest {
    private static WireMockServer wireMockServer;
    private static String wireMockHost;

    final static Client CLIENT = HttpClient.newClient(new ClientConfig()
            .register(new JacksonFeature()));
    static RecordServiceConnector connector;

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
        connector = new RecordServiceConnector(CLIENT, wireMockHost, RecordServiceConnector.TimingLogLevel.INFO);
    }

    @AfterAll
    static void stopWireMockServer() {
        wireMockServer.stop();
    }

    @Test
    void params() {
        final RecordServiceConnector.Params params = new RecordServiceConnector.Params();
        params.withAllowDeleted(true);
        assertThat("param set",
                params.getAllowDeleted().isPresent(), is(true));
        assertThat("param value",
                params.getAllowDeleted().get(), is(true));
        params.withAllowDeleted(null);
        assertThat("param removed on null",
                params.getAllowDeleted().isPresent(), is(false));
    }

    @Test
    void callRecordExistsForExistingRecord() throws RecordServiceConnectorException {
        assertThat(connector.recordExists("870979", "68135699"),
                is(true));
    }

    @Test
    void callRecordExistsForNonExistingRecord() throws RecordServiceConnectorException {
        final RecordServiceConnector.Params params = new RecordServiceConnector.Params()
                .withAllowDeleted(true);
        assertThat(connector.recordExists("870979", "NoSuchRecord", params),
                is(false));
    }

    @Test
    void callGetRecordContentForExistingRecord() throws RecordServiceConnectorException {
        assertThat(connector.getRecordContent("870979", "68135699"),
                is(notNullValue()));
    }

    @Test
    void callGetRecordDataForExistingRecord() throws RecordServiceConnectorException {
        final RecordData record = connector.getRecordData("870970", "52880645");
        assertThat(record, is(notNullValue()));
        assertThat(record.getRecordId(), is(notNullValue()));
        assertThat(record.getRecordId().getAgencyId(), is(870970));
        assertThat(record.getRecordId().getBibliographicRecordId(), is("52880645"));
        assertThat(record.isDeleted(), is(false));
        assertThat(record.getMimetype(), is("text/marcxchange"));
        assertThat(record.getCreated(), is("2017-01-16T23:00:00Z"));
        assertThat(record.getModified(), is("2018-06-01T13:43:13.147Z"));
        assertThat(record.getTrackingId(), is("{52880645:870970}-68944211-{52880645:870970}"));
        assertThat(new String(record.getContent()), containsString("lokomotivmænd i krig"));
        assertThat(record.getEnrichmentTrail(), is("870970"));
    }

    @Test
    void callGetRecordDataCollection() throws RecordServiceConnectorException {
        final RecordServiceConnector.Params params = new RecordServiceConnector.Params()
                .withAllowDeleted(true)
                .withMode(RecordServiceConnector.Params.Mode.EXPANDED);
        final HashMap<String, RecordData> recordCollection = connector.getRecordDataCollection("870970", "52880645", params);
        assertThat(recordCollection.size(), is(2));
        assertThat("Record from 870970", recordCollection.values().stream().anyMatch(r -> r.getRecordId().getAgencyId() == 870970));
        assertThat("Record from 870979", recordCollection.values().stream().anyMatch(r -> r.getRecordId().getAgencyId() == 870979));
    }

    @Test
    void callGetRecordMetaForExistingRecord() throws RecordServiceConnectorException {
        final RecordData recordMeta = connector.getRecordMeta("870970", "52880645");
        assertThat(recordMeta, is(notNullValue()));
        assertThat(recordMeta.getRecordId(), is(notNullValue()));
        assertThat(recordMeta.getRecordId().getAgencyId(), is(870970));
        assertThat(recordMeta.getRecordId().getBibliographicRecordId(), is("52880645"));
        assertThat(recordMeta.isDeleted(), is(false));
        assertThat(recordMeta.getMimetype(), is("text/marcxchange"));
        assertThat(recordMeta.getCreated(), is("2017-01-16T23:00:00Z"));
        assertThat(recordMeta.getModified(), is("2018-06-01T13:43:13.147Z"));
        assertThat(recordMeta.getTrackingId(), is("{52880645:870970}-68944211-{52880645:870970}"));
        assertThat(recordMeta.getContent(), is(nullValue()));
        assertThat(recordMeta.getEnrichmentTrail(), is("870970"));
    }

    @Test
    void callGetRecordDataIdArgIsNullThrows() {
        assertThrows(NullPointerException.class, () -> connector.getRecordData(null));
    }

    @Test
    void callGetRecordDataCollectionIdArgIsNullThrows() {
        assertThrows(NullPointerException.class, () -> connector.getRecordDataCollection(null));
    }

    @Test
    void callGetRecordParents() throws RecordServiceConnectorException {
        RecordId[] ids = connector.getRecordParents("870970", "44816687");
        assertThat(ids, arrayContaining(new RecordId("44783851", 870970)));
    }

    @Test
    void callGetRecordChildren() throws RecordServiceConnectorException {
        RecordId[] ids = connector.getRecordChildren("870970", "44783851");
        assertThat(ids, arrayContaining(new RecordId("44741172", 870970),
                new RecordId("44816660", 870970),
                new RecordId("44816679", 870970),
                new RecordId("44816687", 870970),
                new RecordId("44871106", 870970),
                new RecordId("45015920", 870970)));
    }

    @Test
    void callRetRecordHistory() throws RecordServiceConnectorException {
        RecordHistoryCollection dto = connector.getRecordHistory("870970", "44783851");

        assertThat(dto, is(notNullValue()));
        assertThat(dto.getRecordHistoryList(), is(notNullValue()));
        assertThat(dto.getRecordHistoryList().size(), is(2));

        RecordHistory historyDTO1 = dto.getRecordHistoryList().get(0);
        assertThat(historyDTO1.getId().getAgencyId(), is(870970));
        assertThat(historyDTO1.getId().getBibliographicRecordId(), is("44783851"));
        assertThat(historyDTO1.isDeleted(), is(false));
        assertThat(historyDTO1.getMimeType(), is("text/marcxchange"));
        assertThat(historyDTO1.getCreated(), is("2010-01-27T23:00:00Z"));
        assertThat(historyDTO1.getModified(), is("2016-06-15T08:58:06.640Z"));
        assertThat(historyDTO1.getTrackingId(), is(""));

        RecordHistory historyDTO2 = dto.getRecordHistoryList().get(1);
        assertThat(historyDTO2.getId().getAgencyId(), is(870970));
        assertThat(historyDTO2.getId().getBibliographicRecordId(), is("44783851"));
        assertThat(historyDTO2.isDeleted(), is(false));
        assertThat(historyDTO2.getMimeType(), is("text/enrichment+marcxchange"));
        assertThat(historyDTO2.getCreated(), is("2010-01-27T23:00:00Z"));
        assertThat(historyDTO2.getModified(), is("2015-03-16T23:35:30.467032Z"));
        assertThat(historyDTO2.getTrackingId(), is(""));
    }
}