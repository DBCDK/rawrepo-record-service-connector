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
import org.junit.jupiter.api.Assertions;
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
    void callGetRecordContentCollectionForExistingRecord() throws RecordServiceConnectorException {
        String expected = "<?xml version='1.0' encoding='UTF-8'?>\n" +
                "<collection xmlns='info:lc/xmlns/marcxchange-v1' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'\n" +
                "            xsi:schemaLocation='info:lc/xmlns/marcxchange-v1 http://www.loc.gov/standards/iso25577/marcxchange-1-1.xsd'>\n" +
                "    <record>\n" +
                "        <leader>00000n 2200000 4500</leader>\n" +
                "        <datafield ind1='0' ind2='0' tag='001'>\n" +
                "            <subfield code='a'>68135699</subfield>\n" +
                "            <subfield code='b'>870979</subfield>\n" +
                "            <subfield code='c'>20160617172944</subfield>\n" +
                "            <subfield code='d'>20131129</subfield>\n" +
                "            <subfield code='f'>a</subfield>\n" +
                "            <subfield code='t'>faust</subfield>\n" +
                "        </datafield>\n" +
                "        <datafield ind1='0' ind2='0' tag='004'>\n" +
                "            <subfield code='r'>n</subfield>\n" +
                "            <subfield code='a'>e</subfield>\n" +
                "            <subfield code='x'>n</subfield>\n" +
                "        </datafield>\n" +
                "    </record>\n" +
                "</collection>";

        assertThat(connector.getRecordContentCollection("870979", "68135699"),
                is(expected.getBytes()));
    }

    @Test
    void callGetRecordContentCollection() throws RecordServiceConnectorException {
        final String expected = "<?xml version='1.0' encoding='UTF-8'?>\n" +
                "<collection xmlns='info:lc/xmlns/marcxchange-v1' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'\n" +
                "            xsi:schemaLocation='info:lc/xmlns/marcxchange-v1 http://www.loc.gov/standards/iso25577/marcxchange-1-1.xsd'>\n" +
                "    <record format=\"danMARC2\" type=\"Bibliographic\">\n" +
                "        <leader>00000n 2200000 4500</leader>\n" +
                "        <datafield tag=\"001\" ind1=\"0\" ind2=\"0\">\n" +
                "            <subfield code=\"a\">05395720</subfield>\n" +
                "            <subfield code=\"b\">770600</subfield>\n" +
                "            <subfield code=\"c\">19940328</subfield>\n" +
                "            <subfield code=\"d\">19790614</subfield>\n" +
                "            <subfield code=\"f\">a</subfield>\n" +
                "            <subfield code=\"o\">c</subfield>\n" +
                "        </datafield>\n" +
                "        <datafield tag=\"004\" ind1=\"0\" ind2=\"0\">\n" +
                "            <subfield code=\"r\">d</subfield>\n" +
                "        </datafield>\n" +
                "        <datafield tag=\"014\" ind1=\"0\" ind2=\"0\">\n" +
                "            <subfield code=\"a\">50129691</subfield>\n" +
                "        </datafield>\n" +
                "        <datafield tag=\"996\" ind1=\"0\" ind2=\"0\">\n" +
                "            <subfield code=\"a\">DBC</subfield>\n" +
                "        </datafield>\n" +
                "    </record>\n" +
                "    <record format=\"danMARC2\" type=\"Bibliographic\">\n" +
                "        <leader>00000n 2200000 4500</leader>\n" +
                "        <datafield tag=\"001\" ind1=\"0\" ind2=\"0\">\n" +
                "            <subfield code=\"a\">50129691</subfield>\n" +
                "            <subfield code=\"b\">870970</subfield>\n" +
                "            <subfield code=\"c\">19920212</subfield>\n" +
                "            <subfield code=\"d\">19790614</subfield>\n" +
                "            <subfield code=\"f\">a</subfield>\n" +
                "            <subfield code=\"o\">c</subfield>\n" +
                "        </datafield>\n" +
                "        <datafield tag=\"004\" ind1=\"0\" ind2=\"0\">\n" +
                "            <subfield code=\"r\">n</subfield>\n" +
                "            <subfield code=\"a\">h</subfield>\n" +
                "        </datafield>\n" +
                "        <datafield tag=\"996\" ind1=\"0\" ind2=\"0\">\n" +
                "            <subfield code=\"a\">DBC</subfield>\n" +
                "        </datafield>\n" +
                "    </record>\n" +
                "</collection>";

        final RecordServiceConnector.Params params = new RecordServiceConnector.Params()
                .withAllowDeleted(true)
                .withUseParentAgency(false)
                .withKeepAutFields(false)
                .withExpand(true);

        assertThat(connector.getRecordContentCollection(770600, "05395721", params),
                is(expected.getBytes()));
    }

    @Test
    void callGetRecordContentCollection_DataIO() throws RecordServiceConnectorException {
        final String expected = "<?xml version='1.0' encoding='UTF-8'?>\n" +
                "<collection xmlns='info:lc/xmlns/marcxchange-v1' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'\n" +
                "            xsi:schemaLocation='info:lc/xmlns/marcxchange-v1 http://www.loc.gov/standards/iso25577/marcxchange-1-1.xsd'>\n" +
                "    <record>\n" +
                "        <leader>00000n 2200000 4500</leader>\n" +
                "        <datafield ind1='0' ind2='0' tag='001'>\n" +
                "            <subfield code='a'>51563697</subfield>\n" +
                "            <subfield code='b'>761500</subfield>\n" +
                "            <subfield code='c'>20160413152805</subfield>\n" +
                "            <subfield code='d'>20150129</subfield>\n" +
                "            <subfield code='f'>a</subfield>\n" +
                "        </datafield>\n" +
                "        <datafield ind1='0' ind2='0' tag='004'>\n" +
                "            <subfield code='r'>n</subfield>\n" +
                "            <subfield code='a'>e</subfield>\n" +
                "        </datafield>\n" +
                "        <datafield ind1='0' ind2='0' tag='005'>\n" +
                "            <subfield code='z'>p</subfield>\n" +
                "        </datafield>\n" +
                "        <datafield ind1='0' ind2='0' tag='008'>\n" +
                "            <subfield code='t'>s</subfield>\n" +
                "            <subfield code='u'>u</subfield>\n" +
                "            <subfield code='a'>2015</subfield>\n" +
                "            <subfield code='b'>dk</subfield>\n" +
                "            <subfield code='d'>2</subfield>\n" +
                "            <subfield code='d'>x</subfield>\n" +
                "            <subfield code='j'>p</subfield>\n" +
                "            <subfield code='l'>dan</subfield>\n" +
                "            <subfield code='o'>b</subfield>\n" +
                "            <subfield code='v'>0</subfield>\n" +
                "        </datafield>\n" +
                "        <datafield ind1='0' ind2='0' tag='996'>\n" +
                "            <subfield code='a'>DBC</subfield>\n" +
                "        </datafield>\n" +
                "    </record>\n" +
                "</collection>";

        final RecordServiceConnector.Params params = new RecordServiceConnector.Params()
                .withExpand(true);

        assertThat(connector.getRecordContentCollectionDataIO(761500, "51563697", params),
                is(expected.getBytes()));
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
    void callGetRecordData_NotFound() {
        Assertions.assertThrows(RecordServiceConnectorNoContentStatusCodeException.class, () -> {
            connector.getRecordData("870970", "NOTFOUND");
        });
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
    void callGetRecordDataCollection_NotFound() {
        final RecordServiceConnector.Params params = new RecordServiceConnector.Params()
                .withAllowDeleted(true)
                .withMode(RecordServiceConnector.Params.Mode.EXPANDED);

        Assertions.assertThrows(RecordServiceConnectorNoContentStatusCodeException.class, () -> {
            connector.getRecordDataCollection("870970", "NOTFOUND", params);
        });
    }

    @Test
    void callGetRecordMeta_ExistingRecord() throws RecordServiceConnectorException {
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
    void callGetRecordMeta_NotFound() {
        Assertions.assertThrows(RecordServiceConnectorNoContentStatusCodeException.class, () -> {
            connector.getRecordMeta("870970", "NOTFOUND");
        });
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
    void callGetRecordParents_NotFound() {
        Assertions.assertThrows(RecordServiceConnectorNoContentStatusCodeException.class, () -> {
            connector.getRecordParents("870970", "NOTFOUND");
        });
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
    public void callGetRecordSiblingsFrom() throws RecordServiceConnectorException {
        RecordId[] ids = connector.getRecordSiblingsFrom(870974, "126350554");

        assertThat(ids.length, is(0));
    }

    @Test
    void callGetRecordSiblingsFrom_NotFound() {
        Assertions.assertThrows(RecordServiceConnectorNoContentStatusCodeException.class, () -> {
            connector.getRecordSiblingsFrom(870970, "NOTFOUND");
        });
    }

    @Test
    public void callGetRecordSiblingsTo() throws RecordServiceConnectorException {
        RecordId[] ids = connector.getRecordSiblingsTo(870974, "126350554");

        assertThat(ids, arrayContaining(new RecordId("126350554", 191919)));
    }

    @Test
    void callGetRecordSiblingsTo_NotFound() {
        Assertions.assertThrows(RecordServiceConnectorNoContentStatusCodeException.class, () -> {
            connector.getRecordSiblingsTo(870970, "NOTFOUND");
        });
    }

    @Test
    void callGetRecordHistory() throws RecordServiceConnectorException {
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

    @Test
    public void callGetHistoricRecord() throws RecordServiceConnectorException {
        RecordData record1 = connector.getHistoricRecord("870970", "44783851", "2016-06-15T08:58:06.640Z");
        assertThat(record1, is(notNullValue()));
        assertThat(record1.getRecordId(), is(notNullValue()));
        assertThat(record1.getRecordId().getAgencyId(), is(870970));
        assertThat(record1.getRecordId().getBibliographicRecordId(), is("44783851"));
        assertThat(record1.isDeleted(), is(false));
        assertThat(record1.getMimetype(), is("text/marcxchange"));
        assertThat(record1.getCreated(), is("2010-01-27T23:00:00Z"));
        assertThat(record1.getModified(), is("2016-06-15T08:58:06.640Z"));
        assertThat(record1.getTrackingId(), is(""));
        assertThat(new String(record1.getContent()), containsString("Forlaget Oktober"));
        assertThat(record1.getEnrichmentTrail(), is("870970"));

        RecordData record2 = connector.getHistoricRecord("870970", "44783851", "2015-03-16T23:35:30.467032Z");
        assertThat(record2, is(notNullValue()));
        assertThat(record2.getRecordId(), is(notNullValue()));
        assertThat(record2.getRecordId().getAgencyId(), is(870970));
        assertThat(record2.getRecordId().getBibliographicRecordId(), is("44783851"));
        assertThat(record2.isDeleted(), is(false));
        assertThat(record2.getMimetype(), is("text/enrichment+marcxchange"));
        assertThat(record2.getCreated(), is("2010-01-27T23:00:00Z"));
        assertThat(record2.getModified(), is("2015-03-16T23:35:30.467032Z"));
        assertThat(record2.getTrackingId(), is(""));
        assertThat(new String(record2.getContent()), containsString("tag=\"s10\""));
        assertThat(record2.getEnrichmentTrail(), is("870970"));
    }

    @Test
    void callGetHistoricRecord_NotFound() {
        Assertions.assertThrows(RecordServiceConnectorNoContentStatusCodeException.class, () -> {
            connector.getHistoricRecord("870970", "NOTFOUND", "2016-06-15T08:58:06.640Z");
        });
    }

}