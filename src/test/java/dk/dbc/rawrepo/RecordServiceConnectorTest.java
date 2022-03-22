/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GPLv3
 * See license text in LICENSE.txt or at https://opensource.dbc.dk/licenses/gpl-3.0/
 */

package dk.dbc.rawrepo;

import com.github.tomakehurst.wiremock.WireMockServer;
import dk.dbc.httpclient.HttpClient;
import dk.dbc.jsonb.JSONBContext;
import dk.dbc.jsonb.JSONBException;
import dk.dbc.rawrepo.dto.RecordCollectionDTOv2;
import dk.dbc.rawrepo.dto.RecordDTO;
import dk.dbc.rawrepo.dto.RecordEntryDTO;
import dk.dbc.rawrepo.dto.RecordHistoryCollectionDTO;
import dk.dbc.rawrepo.dto.RecordHistoryDTO;
import dk.dbc.rawrepo.dto.RecordIdDTO;
import dk.dbc.rawrepo.record.RecordServiceConnector;
import dk.dbc.rawrepo.record.RecordServiceConnectorException;
import dk.dbc.rawrepo.record.RecordServiceConnectorNoContentStatusCodeException;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.ws.rs.client.Client;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

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
    void callGetRecordEntryDTO() throws RecordServiceConnectorException {
        final RecordEntryDTO recordEntryDTO = connector.getRawRecordEntryDTO("870979", "19000001");
        assertThat("get agencyId", recordEntryDTO.getRecordId().getAgencyId(),
                is(870979));
        assertThat("get bibliographicRecordid", recordEntryDTO.getRecordId().getBibliographicRecordId(),
                is("19000001"));
        assertThat("content is MarcJson", recordEntryDTO.getContent().has("leader"),
                is(true));
    }

    @Test
    void callGetRecordEntry() throws RecordServiceConnectorException, JSONBException {
        final byte[] recordEntryBytes = connector.getRawRecordEntry("870979", "19000001");
        final JSONBContext jsonbContext = new JSONBContext();
        final RecordEntryDTO recordEntryDTO = jsonbContext.unmarshall(
                new String(recordEntryBytes, StandardCharsets.UTF_8), RecordEntryDTO.class);
        assertThat("get agencyId", recordEntryDTO.getRecordId().getAgencyId(),
                is(870979));
        assertThat("get bibliographicRecordid", recordEntryDTO.getRecordId().getBibliographicRecordId(),
                is("19000001"));
        assertThat("content is MarcJson", recordEntryDTO.getContent().has("leader"),
                is(true));
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
    void callGetAllAgenciesForBibliographicRecordId() throws RecordServiceConnectorException {
        final Integer[] actual = connector.getAllAgenciesForBibliographicRecordId("27722342");
        assertThat(actual.length, is(4));

        for (Integer agencyId : Arrays.asList(191919, 710100, 761500, 870970)) {
            assertThat(Arrays.asList(actual).contains(agencyId), is(true));
        }
    }

    @Test
    void callGetAllAgenciesForBibliographicRecordId_NoRecord() throws RecordServiceConnectorException {
        final Integer[] actual = connector.getAllAgenciesForBibliographicRecordId("00000000");

        assertThat(actual.length, is(0));
    }

    @Test
    void callGetRecordContentCollection_DataIO() throws RecordServiceConnectorException {
        final RecordServiceConnector.Params params = new RecordServiceConnector.Params()
                .withExpand(true);

        final HashMap<String, RecordDTO> actual = connector.getRecordDataCollectionDataIO(new RecordIdDTO("51563697", 761500), params);
        assertThat(actual.size(), is(1));
        assertThat(actual.get("51563697").getRecordId(), is(new RecordIdDTO("51563697", 761500)));
        assertThat(actual.get("51563697").getCreated(), is("2016-04-13T11:28:06.683Z"));
        assertThat(actual.get("51563697").getEnrichmentTrail(), is("870970,761500"));
        assertThat(actual.get("51563697").getMimetype(), is("text/marcxchange"));
        assertThat(actual.get("51563697").getModified(), is("2020-02-24T13:15:47.945Z"));
        assertThat(actual.get("51563697").getTrackingId(), is(""));
        assertThat(actual.get("51563697").isDeleted(), is(false));
        assertThat(new String(actual.get("51563697").getContent()), containsString("marcxchange-v1"));
    }

    @Test
    void callGetRecordContentCollection_DataIO_handleControlRecords() throws RecordServiceConnectorException {
        final RecordServiceConnector.Params params = new RecordServiceConnector.Params()
                .withExpand(true)
                .withHandleControlRecords(true);

        final HashMap<String, RecordDTO> actual = connector.getRecordDataCollectionDataIO(new RecordIdDTO("38519387", 191919), params);
        assertThat(actual.size(), is(2));

        assertThat(actual.get("38519387").getRecordId(), is(new RecordIdDTO("38519387", 191919)));
        assertThat(actual.get("38519387").getCreated(), is("2020-12-11T11:08:07.010008Z"));
        assertThat(actual.get("38519387").getEnrichmentTrail(), is("870970,191919"));
        assertThat(actual.get("38519387").getMimetype(), is("text/marcxchange"));
        assertThat(actual.get("38519387").getModified(), is("2021-06-30T07:30:16.630495Z"));
        assertThat(actual.get("38519387").getTrackingId(), is("{38519387:bog} - kga-{38519387:191919}"));
        assertThat(actual.get("38519387").isDeleted(), is(false));
        assertThat(new String(actual.get("38519387").getContent()), containsString("marcxchange-v1"));

        assertThat(actual.get("28947216").getRecordId(), is(new RecordIdDTO("28947216", 191919)));
        assertThat(actual.get("28947216").getCreated(), is("2016-07-09T09:15:57.550Z"));
        assertThat(actual.get("28947216").getEnrichmentTrail(), is("870970,191919"));
        assertThat(actual.get("28947216").getMimetype(), is("text/marcxchange"));
        assertThat(actual.get("28947216").getModified(), is("2021-06-30T07:30:17.173666Z"));
        assertThat(actual.get("28947216").getTrackingId(), is("{68594693:autoritet} - kfm-{28947216:191919}"));
        assertThat(actual.get("28947216").isDeleted(), is(false));
        assertThat(new String(actual.get("28947216").getContent()), containsString("marcxchange-v1"));
    }

    @Test
    void callGetRecordDataForExistingRecord() throws RecordServiceConnectorException {
        final RecordDTO record = connector.getRecordData("870970", "52880645");
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
        final HashMap<String, RecordDTO> recordCollection = connector.getRecordDataCollection("870970", "52880645", params);
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
        final RecordDTO recordMeta = connector.getRecordMeta("870970", "52880645");
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
        RecordIdDTO[] ids = connector.getRecordParents("870970", "44816687");
        assertThat(ids, arrayContaining(new RecordIdDTO("44783851", 870970)));
    }

    @Test
    void callGetRecordParents_NotFound() {
        Assertions.assertThrows(RecordServiceConnectorNoContentStatusCodeException.class, () -> {
            connector.getRecordParents("870970", "NOTFOUND");
        });
    }

    @Test
    void callGetRecordChildren() throws RecordServiceConnectorException {
        RecordIdDTO[] ids = connector.getRecordChildren("870970", "44783851");
        assertThat(ids, arrayContaining(new RecordIdDTO("44741172", 870970),
                new RecordIdDTO("44816660", 870970),
                new RecordIdDTO("44816679", 870970),
                new RecordIdDTO("44816687", 870970),
                new RecordIdDTO("44871106", 870970),
                new RecordIdDTO("45015920", 870970)));
    }

    @Test
    public void callGetRecordSiblingsFrom() throws RecordServiceConnectorException {
        RecordIdDTO[] ids = connector.getRecordSiblingsFrom(870974, "126350554");

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
        RecordIdDTO[] ids = connector.getRecordSiblingsTo(870974, "126350554");

        assertThat(ids, arrayContaining(new RecordIdDTO("126350554", 191919)));
    }

    @Test
    void callGetRecordSiblingsTo_NotFound() {
        Assertions.assertThrows(RecordServiceConnectorNoContentStatusCodeException.class, () -> {
            connector.getRecordSiblingsTo(870970, "NOTFOUND");
        });
    }

    @Test
    void callGetRecordHistory() throws RecordServiceConnectorException {
        RecordHistoryCollectionDTO dto = connector.getRecordHistory("870970", "44783851");

        assertThat(dto, is(notNullValue()));
        assertThat(dto.getRecordHistoryList(), is(notNullValue()));
        assertThat(dto.getRecordHistoryList().size(), is(2));

        RecordHistoryDTO historyDTO1 = dto.getRecordHistoryList().get(0);
        assertThat(historyDTO1.getId().getAgencyId(), is(870970));
        assertThat(historyDTO1.getId().getBibliographicRecordId(), is("44783851"));
        assertThat(historyDTO1.isDeleted(), is(false));
        assertThat(historyDTO1.getMimeType(), is("text/marcxchange"));
        assertThat(historyDTO1.getCreated(), is("2010-01-27T23:00:00Z"));
        assertThat(historyDTO1.getModified(), is("2016-06-15T08:58:06.640Z"));
        assertThat(historyDTO1.getTrackingId(), is(""));

        RecordHistoryDTO historyDTO2 = dto.getRecordHistoryList().get(1);
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
        RecordDTO record1 = connector.getHistoricRecord("870970", "44783851", "2016-06-15T08:58:06.640Z");
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

        RecordDTO record2 = connector.getHistoricRecord("870970", "44783851", "2015-03-16T23:35:30.467032Z");
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

    @Test
    void fetchRecordList_NoParams() throws RecordServiceConnectorException {
        final List<RecordIdDTO> recordIds = new ArrayList<>();
        recordIds.add(new RecordIdDTO("55103461", 870970));
        recordIds.add(new RecordIdDTO("54936931", 870970));
        recordIds.add(new RecordIdDTO("missing", 123456));

        RecordCollectionDTOv2 actual = connector.fetchRecordList(recordIds);

        RecordDTO recordDTO = actual.getFound().get(0);
        assertThat("collection contains 55103461", recordDTO.getRecordId(), is(new RecordIdDTO("55103461", 870970)));
        assertThat("collection mimetype 55103461", recordDTO.getMimetype(), is("text/marcxchange"));
        assertThat("collection created 55103461", recordDTO.getCreated(), is("2018-10-25T09:41:55.891Z"));
        assertThat("collection modified 55103461", recordDTO.getModified(), is("2018-10-25T09:41:55.891Z"));
        assertThat("collection enrichment trail 55103461", recordDTO.getEnrichmentTrail(), is("870970"));
        assertThat("collection content 55103461", new String(recordDTO.getContent()), is("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><record xsi:schemaLocation=\"http://www.loc.gov/standards/iso25577/marcxchange-1-1.xsd\" xmlns=\"info:lc/xmlns/marcxchange-v1\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"><leader>00000n    2200000   4500</leader><datafield tag=\"001\" ind1=\"0\" ind2=\"0\"><subfield code=\"a\">55103461</subfield><subfield code=\"b\">870970</subfield><subfield code=\"c\">20181025114155</subfield><subfield code=\"d\">20181025</subfield><subfield code=\"f\">a</subfield></datafield><datafield tag=\"004\" ind1=\"0\" ind2=\"0\"><subfield code=\"r\">c</subfield><subfield code=\"a\">b</subfield></datafield><datafield tag=\"008\" ind1=\"0\" ind2=\"0\"><subfield code=\"t\">m</subfield><subfield code=\"u\">f</subfield><subfield code=\"a\">2018</subfield><subfield code=\"v\">0</subfield></datafield><datafield tag=\"014\" ind1=\"0\" ind2=\"0\"><subfield code=\"a\">44304937</subfield></datafield><datafield tag=\"021\" ind1=\"0\" ind2=\"0\"><subfield code=\"e\">9781421596020</subfield><subfield code=\"d\">85</subfield></datafield><datafield tag=\"245\" ind1=\"0\" ind2=\"0\"><subfield code=\"G\">74</subfield><subfield code=\"g\">Vol. 74</subfield><subfield code=\"a\">Death &amp; strawberry</subfield></datafield><datafield tag=\"250\" ind1=\"0\" ind2=\"0\"><subfield code=\"x\">1. printing</subfield></datafield><datafield tag=\"260\" ind1=\"0\" ind2=\"0\"><subfield code=\"c\">2018</subfield></datafield><datafield tag=\"300\" ind1=\"0\" ind2=\"0\"><subfield code=\"a\">Ca. 180 sider</subfield></datafield><datafield tag=\"504\" ind1=\"0\" ind2=\"0\"><subfield code=\"a\">The final battle against Yhwach gets under way as Ichigo and his allies reach the Quincy King’s throne room. Can Ichigo put an end to the thousand-year war between the Soul Reapers and Quincies? The emotional conclusion of Bleach is here!</subfield></datafield><datafield tag=\"996\" ind1=\"0\" ind2=\"0\"><subfield code=\"a\">717500</subfield></datafield></record>"));

        recordDTO = actual.getFound().get(1);
        assertThat("collection contains 54936931", recordDTO.getRecordId(), is(new RecordIdDTO("54936931", 870970)));
        assertThat("collection mimetype 54936931", recordDTO.getMimetype(), is("text/marcxchange"));
        assertThat("collection created 54936931", recordDTO.getCreated(), is("2018-09-28T08:35:23.977Z"));
        assertThat("collection modified 54936931", recordDTO.getModified(), is("2018-09-28T08:35:23.977Z"));
        assertThat("collection enrichment trail 54936931", recordDTO.getEnrichmentTrail(), Matchers.is("870970"));
        assertThat("collection content 54936931", new String(recordDTO.getContent()), is("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><record xsi:schemaLocation=\"http://www.loc.gov/standards/iso25577/marcxchange-1-1.xsd\" xmlns=\"info:lc/xmlns/marcxchange-v1\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"><leader>00000n    2200000   4500</leader><datafield tag=\"001\" ind1=\"0\" ind2=\"0\"><subfield code=\"a\">54936931</subfield><subfield code=\"b\">870970</subfield><subfield code=\"c\">20180928103523</subfield><subfield code=\"d\">20180928</subfield><subfield code=\"f\">a</subfield></datafield><datafield tag=\"004\" ind1=\"0\" ind2=\"0\"><subfield code=\"r\">c</subfield><subfield code=\"a\">b</subfield></datafield><datafield tag=\"008\" ind1=\"0\" ind2=\"0\"><subfield code=\"t\">m</subfield><subfield code=\"u\">f</subfield><subfield code=\"a\">2018</subfield><subfield code=\"v\">0</subfield></datafield><datafield tag=\"014\" ind1=\"0\" ind2=\"0\"><subfield code=\"a\">44304937</subfield></datafield><datafield tag=\"021\" ind1=\"0\" ind2=\"0\"><subfield code=\"e\">9781421594347</subfield><subfield code=\"d\">85</subfield></datafield><datafield tag=\"245\" ind1=\"0\" ind2=\"0\"><subfield code=\"G\">73</subfield><subfield code=\"g\">Vol. 73</subfield><subfield code=\"a\">Battlefield burning</subfield></datafield><datafield tag=\"250\" ind1=\"0\" ind2=\"0\"><subfield code=\"x\">1. printing</subfield></datafield><datafield tag=\"260\" ind1=\"0\" ind2=\"0\"><subfield code=\"c\">2018</subfield></datafield><datafield tag=\"300\" ind1=\"0\" ind2=\"0\"><subfield code=\"a\">Ca. 180 sider</subfield></datafield><datafield tag=\"504\" ind1=\"0\" ind2=\"0\"><subfield code=\"a\">Facing a powerful opponent, the mysterious Kisuke Urahara is forced to reveal his Bankai for the first time. Meanwhile, Ichigo finally makes it to Yhwach’s throne room, but what can he do against an enemy whose power is omnipotence?!</subfield></datafield><datafield tag=\"996\" ind1=\"0\" ind2=\"0\"><subfield code=\"a\">717500</subfield></datafield></record>"));

        assertThat("collection is missing record", actual.getMissing().size(), is(1));
        assertThat("collection is missing record", actual.getMissing().get(0), is(new RecordIdDTO("missing", 123456)));
    }

    @Test
    void fetchRecordList_Merged() throws RecordServiceConnectorException {
        final List<RecordIdDTO> recordIds = new ArrayList<>();
        recordIds.add(new RecordIdDTO("55103461", 191919));
        recordIds.add(new RecordIdDTO("54936931", 191919));
        recordIds.add(new RecordIdDTO("missing", 123456));

        RecordServiceConnector.Params params = new RecordServiceConnector.Params()
                .withAllowDeleted(true)
                .withMode(RecordServiceConnector.Params.Mode.EXPANDED)
                .withUseParentAgency(true);

        RecordCollectionDTOv2 actual = connector.fetchRecordList(recordIds, params);

        RecordDTO recordDTO = actual.getFound().get(0);
        assertThat("collection contains 55103461", recordDTO.getRecordId(), is(new RecordIdDTO("55103461", 191919)));
        assertThat("collection mimetype 55103461", recordDTO.getMimetype(), is("text/marcxchange"));
        assertThat("collection created 55103461", recordDTO.getCreated(), is("2018-10-25T09:41:57.251Z"));
        assertThat("collection enrichment trail 55103461", recordDTO.getEnrichmentTrail(), is("870970,191919"));
        assertThat("collection content 55103461", new String(recordDTO.getContent()), is("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><record xsi:schemaLocation=\"http://www.loc.gov/standards/iso25577/marcxchange-1-1.xsd\" xmlns=\"info:lc/xmlns/marcxchange-v1\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"><leader>00000n    2200000   4500</leader><datafield tag=\"001\" ind1=\"0\" ind2=\"0\"><subfield code=\"a\">55103461</subfield><subfield code=\"b\">870970</subfield><subfield code=\"c\">20181025114155</subfield><subfield code=\"d\">20181025</subfield><subfield code=\"f\">a</subfield></datafield><datafield tag=\"004\" ind1=\"0\" ind2=\"0\"><subfield code=\"r\">c</subfield><subfield code=\"a\">b</subfield></datafield><datafield tag=\"008\" ind1=\"0\" ind2=\"0\"><subfield code=\"t\">m</subfield><subfield code=\"u\">f</subfield><subfield code=\"a\">2018</subfield><subfield code=\"v\">0</subfield></datafield><datafield tag=\"014\" ind1=\"0\" ind2=\"0\"><subfield code=\"a\">44304937</subfield></datafield><datafield tag=\"021\" ind1=\"0\" ind2=\"0\"><subfield code=\"e\">9781421596020</subfield><subfield code=\"d\">85</subfield></datafield><datafield tag=\"245\" ind1=\"0\" ind2=\"0\"><subfield code=\"G\">74</subfield><subfield code=\"g\">Vol. 74</subfield><subfield code=\"a\">Death &amp; strawberry</subfield></datafield><datafield tag=\"250\" ind1=\"0\" ind2=\"0\"><subfield code=\"x\">1. printing</subfield></datafield><datafield tag=\"260\" ind1=\"0\" ind2=\"0\"><subfield code=\"c\">2018</subfield></datafield><datafield tag=\"300\" ind1=\"0\" ind2=\"0\"><subfield code=\"a\">Ca. 180 sider</subfield></datafield><datafield tag=\"504\" ind1=\"0\" ind2=\"0\"><subfield code=\"a\">The final battle against Yhwach gets under way as Ichigo and his allies reach the Quincy King’s throne room. Can Ichigo put an end to the thousand-year war between the Soul Reapers and Quincies? The emotional conclusion of Bleach is here!</subfield></datafield><datafield tag=\"996\" ind1=\"0\" ind2=\"0\"><subfield code=\"a\">717500</subfield></datafield></record>"));

        recordDTO = actual.getFound().get(1);
        assertThat("collection contains 54936931", recordDTO.getRecordId(), is(new RecordIdDTO("54936931", 191919)));
        assertThat("collection mimetype 54936931", recordDTO.getMimetype(), is("text/marcxchange"));
        assertThat("collection created 54936931", recordDTO.getCreated(), is("2018-09-28T08:35:24.453Z"));
        assertThat("collection enrichment trail 54936931", recordDTO.getEnrichmentTrail(), Matchers.is("870970,191919"));
        assertThat("collection content 54936931", new String(recordDTO.getContent()), is("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><record xsi:schemaLocation=\"http://www.loc.gov/standards/iso25577/marcxchange-1-1.xsd\" xmlns=\"info:lc/xmlns/marcxchange-v1\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"><leader>00000n    2200000   4500</leader><datafield tag=\"001\" ind1=\"0\" ind2=\"0\"><subfield code=\"a\">54936931</subfield><subfield code=\"b\">870970</subfield><subfield code=\"c\">20180928103523</subfield><subfield code=\"d\">20180928</subfield><subfield code=\"f\">a</subfield></datafield><datafield tag=\"004\" ind1=\"0\" ind2=\"0\"><subfield code=\"r\">c</subfield><subfield code=\"a\">b</subfield></datafield><datafield tag=\"008\" ind1=\"0\" ind2=\"0\"><subfield code=\"t\">m</subfield><subfield code=\"u\">f</subfield><subfield code=\"a\">2018</subfield><subfield code=\"v\">0</subfield></datafield><datafield tag=\"014\" ind1=\"0\" ind2=\"0\"><subfield code=\"a\">44304937</subfield></datafield><datafield tag=\"021\" ind1=\"0\" ind2=\"0\"><subfield code=\"e\">9781421594347</subfield><subfield code=\"d\">85</subfield></datafield><datafield tag=\"245\" ind1=\"0\" ind2=\"0\"><subfield code=\"G\">73</subfield><subfield code=\"g\">Vol. 73</subfield><subfield code=\"a\">Battlefield burning</subfield></datafield><datafield tag=\"250\" ind1=\"0\" ind2=\"0\"><subfield code=\"x\">1. printing</subfield></datafield><datafield tag=\"260\" ind1=\"0\" ind2=\"0\"><subfield code=\"c\">2018</subfield></datafield><datafield tag=\"300\" ind1=\"0\" ind2=\"0\"><subfield code=\"a\">Ca. 180 sider</subfield></datafield><datafield tag=\"504\" ind1=\"0\" ind2=\"0\"><subfield code=\"a\">Facing a powerful opponent, the mysterious Kisuke Urahara is forced to reveal his Bankai for the first time. Meanwhile, Ichigo finally makes it to Yhwach’s throne room, but what can he do against an enemy whose power is omnipotence?!</subfield></datafield><datafield tag=\"996\" ind1=\"0\" ind2=\"0\"><subfield code=\"a\">717500</subfield></datafield></record>"));

        assertThat("collection is missing record", actual.getMissing().size(), is(1));
        assertThat("collection is missing record", actual.getMissing().get(0), is(new RecordIdDTO("missing", 123456)));
    }

}