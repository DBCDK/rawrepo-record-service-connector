/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GPLv3
 * See license text in LICENSE.txt or at https://opensource.dbc.dk/licenses/gpl-3.0/
 */

package dk.dbc.rawrepo;

import com.github.tomakehurst.wiremock.WireMockServer;
import dk.dbc.httpclient.HttpClient;
import dk.dbc.rawrepo.agency.RecordAgencyServiceConnector;
import dk.dbc.rawrepo.agency.RecordAgencyServiceConnectorException;
import dk.dbc.rawrepo.dto.RecordIdDTO;
import dk.dbc.rawrepo.dto.RecordIdCollectionDTO;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.ws.rs.client.Client;
import java.util.Arrays;

import static com.github.tomakehurst.wiremock.client.WireMock.configureFor;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class RecordAgencyServiceConnectorTest {
    private static WireMockServer wireMockServer;
    private static String wireMockHost;

    final static Client CLIENT = HttpClient.newClient(new ClientConfig()
            .register(new JacksonFeature()));
    static RecordAgencyServiceConnector connector;

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
        connector = new RecordAgencyServiceConnector(CLIENT, wireMockHost, RecordAgencyServiceConnector.TimingLogLevel.INFO);
    }

    @AfterAll
    static void stopWireMockServer() {
        wireMockServer.stop();
    }

    @Test
    void callGetAllAgencies() throws RecordAgencyServiceConnectorException {
        Integer[] actual = connector.getAllAgencies();
        assertThat(actual.length, is(12));

        for (Integer agencyId : Arrays.asList(190002, 190004, 191919, 710100, 733000, 758000, 761500, 870970, 870971, 870974, 870976, 870979)) {
            assertThat(Arrays.asList(actual).contains(agencyId), is(true));
        }
    }

    @Test
    void callGetBibliographicRecordIdsForAgencyId() throws RecordAgencyServiceConnectorException {
        RecordIdCollectionDTO actual = connector.getBibliographicRecordIdsForAgencyId("710100");

        assertThat(actual.getRecordIds().size(), is(8));

        for (String bibliographicRecordId : Arrays.asList("27722342", "28920806", "49345011", "52871948", "54316267", "54338600", "54730837", "54800053")) {
            assertThat(actual.getRecordIds().contains(new RecordIdDTO(bibliographicRecordId, 710100)), is(true));
        }
    }

    @Test
    void callGetBibliographicRecordIdsForAgencyId_NoRecords() throws RecordAgencyServiceConnectorException {
        RecordIdCollectionDTO actual = connector.getBibliographicRecordIdsForAgencyId("000000");

        assertThat(actual.getRecordIds().size(), is(0));
    }
}
