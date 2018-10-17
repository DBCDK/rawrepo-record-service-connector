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

import java.sql.SQLException;
import java.util.HashMap;

import static com.github.tomakehurst.wiremock.client.WireMock.configureFor;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

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
        connector = new RecordServiceConnector(CLIENT, wireMockHost);
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
        final RecordData record = connector.getRecordData ("870970", "52880645");
        assertThat(record, is(notNullValue ()));
        assertThat(record.getRecordId(), is(notNullValue()));
        assertThat(record.getRecordId().getAgencyId (), is(870970));
        assertThat(record.getRecordId().getBibliographicRecordId(), is("52880645"));
        assertThat(record.isDeleted(), is(false));
        assertThat(record.getMimetype(), is("text/marcxchange"));
        assertThat(record.getCreated(), is("2017-01-16T23:00:00Z"));
        assertThat(record.getModified(), is("2018-06-01T13:43:13.147Z"));
        assertThat(record.getTrackingId(), is("{52880645:870970}-68944211-{52880645:870970}"));
        assertThat(new String(record.getContent()), containsString ("lokomotivmænd i krig"));
        assertThat(record.getEnrichmentTrail(), is("870970"));
    }

    @Test
    void callGetRecordDataCollection() throws RecordServiceConnectorException {
        final RecordServiceConnector.Params params = new RecordServiceConnector.Params()
                .withAllowDeleted(true)
                .withMode (RecordServiceConnector.Params.Mode.EXPANDED);
        final HashMap<String, RecordData> recordCollection = connector.getRecordDataCollection ("870970", "52880645", params);
        assertThat(recordCollection.size (), is(2));
        assertThat("Record from 870970", recordCollection.values ().stream().filter(s -> s.getRecordId ().getAgencyId () == 870970).findFirst().isPresent());
        assertThat("Record from 870979", recordCollection.values ().stream().filter(s -> s.getRecordId ().getAgencyId () == 870979).findFirst().isPresent());
    }

    @Test
    void callGetRecordMetaForExistingRecord() throws RecordServiceConnectorException {
        final RecordData recordMeta = connector.getRecordMeta ("870970", "52880645");
        assertThat(recordMeta, is(notNullValue ()));
        assertThat(recordMeta.getRecordId(), is(notNullValue()));
        assertThat(recordMeta.getRecordId().getAgencyId (), is(870970));
        assertThat(recordMeta.getRecordId().getBibliographicRecordId(), is("52880645"));
        assertThat(recordMeta.isDeleted(), is(false));
        assertThat(recordMeta.getMimetype(), is("text/marcxchange"));
        assertThat(recordMeta.getCreated(), is("2017-01-16T23:00:00Z"));
        assertThat(recordMeta.getModified(), is("2018-06-01T13:43:13.147Z"));
        assertThat(recordMeta.getTrackingId(), is("{52880645:870970}-68944211-{52880645:870970}"));
        assertThat("Content is null", recordMeta.getContent() == null);
        assertThat(recordMeta.getEnrichmentTrail(), is("870970"));
    }

    @Test
    public void callGetRecordDataIdArgIsNullThrows() {
        assertThrows(NullPointerException.class, () -> connector.getRecordData(null, null));
    }

    @Test
    public void callGetRecordDataCollectionIdArgIsNullThrows() {
        assertThrows(NullPointerException.class, () -> connector.getRecordDataCollection(null));
    }
}