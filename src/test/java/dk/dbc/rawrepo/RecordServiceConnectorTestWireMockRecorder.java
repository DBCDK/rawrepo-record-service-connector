/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GPLv3
 * See license text in LICENSE.txt or at https://opensource.dbc.dk/licenses/gpl-3.0/
 */

package dk.dbc.rawrepo;

import java.io.IOException;

public class RecordServiceConnectorTestWireMockRecorder {
    /*
        Steps to reproduce wiremock recording:

        * Start standalone runner
            java -jar wiremock-standalone-{WIRE_MOCK_VERSION}.jar --proxy-all="{RECORD_SERVICE_HOST}" --record-mappings --verbose

        * Run the main method of this class

        * Replace content of src/test/resources/{__files|mappings} with that produced by the standalone runner
     */

    public static void main(String[] args) throws Exception {
        RecordServiceConnectorTest.connector = new RecordServiceConnector(
                RecordServiceConnectorTest.CLIENT, "http://localhost:8080");
        final RecordServiceConnectorTest recordServiceConnectorTest = new RecordServiceConnectorTest();
        final RecordDumpServiceConnectorTest recordRecordDumpServiceConnectorTest =
                new RecordDumpServiceConnectorTest();
        recordRecordExistsRequests(recordServiceConnectorTest);
        recordRelationRequests(recordServiceConnectorTest);
        recordGetRecordContentRequests(recordServiceConnectorTest);
        recordGetRecordDataRequests(recordServiceConnectorTest);
        recordGetRecordDataCollectionRequests(recordServiceConnectorTest);
        recordGetRecordMetaRequests(recordServiceConnectorTest);
        recordHistoryServiceRequests(recordServiceConnectorTest);
        recordAllAgenciesForBibliographicRecordId(recordServiceConnectorTest);
        recordDumpServiceRequests(recordRecordDumpServiceConnectorTest);
    }

    private static void recordRecordExistsRequests(RecordServiceConnectorTest connectorTest)
            throws RecordServiceConnectorException {
        connectorTest.callRecordExistsForExistingRecord();
        connectorTest.callRecordExistsForNonExistingRecord();
    }

    private static void recordRelationRequests(RecordServiceConnectorTest connectorTest)
            throws RecordServiceConnectorException {
        connectorTest.callGetRecordParents();
        connectorTest.callGetRecordParents_NotFound();
        connectorTest.callGetRecordChildren();
        connectorTest.callGetRecordSiblingsFrom();
        connectorTest.callGetRecordSiblingsFrom_NotFound();
        connectorTest.callGetRecordSiblingsTo();
        connectorTest.callGetRecordSiblingsTo_NotFound();
    }

    private static void recordGetRecordContentRequests(RecordServiceConnectorTest connectorTest)
            throws RecordServiceConnectorException {
        connectorTest.callGetRecordContentForExistingRecord();
        connectorTest.callGetRecordContentCollectionForExistingRecord();
        connectorTest.callGetRecordContentCollection();
        connectorTest.callGetRecordContentCollection_DataIO();
    }

    private static void recordGetRecordDataRequests(RecordServiceConnectorTest connectorTest)
            throws RecordServiceConnectorException {
        connectorTest.callGetRecordDataForExistingRecord();
        connectorTest.callGetRecordData_NotFound();
    }

    private static void recordGetRecordDataCollectionRequests(RecordServiceConnectorTest connectorTest)
            throws RecordServiceConnectorException {
        connectorTest.callGetRecordDataCollection();
        connectorTest.callGetRecordDataCollection_NotFound();
    }

    private static void recordGetRecordMetaRequests(RecordServiceConnectorTest connectorTest)
            throws RecordServiceConnectorException {
        connectorTest.callGetRecordMeta_ExistingRecord();
        connectorTest.callGetRecordMeta_NotFound();
    }

    private static void recordHistoryServiceRequests(RecordServiceConnectorTest connectorTest)
            throws RecordServiceConnectorException {
        connectorTest.callGetRecordHistory();
        connectorTest.callGetHistoricRecord();
        connectorTest.callGetHistoricRecord_NotFound();
    }

    private static void recordAllAgenciesForBibliographicRecordId(RecordServiceConnectorTest connectorTest)
            throws RecordServiceConnectorException {
        connectorTest.callGetAllAgenciesForBibliographicRecordId();
        connectorTest.callGetAllAgenciesForBibliographicRecordId_NoRecord();
    }

    private static void recordDumpServiceRequests(RecordDumpServiceConnectorTest recordRecordDumpServiceConnectorTest)
            throws RecordDumpServiceConnectorException, IOException {
        recordRecordDumpServiceConnectorTest.callDumpAgencyDryRun();
        recordRecordDumpServiceConnectorTest.callDumpAgency();
        recordRecordDumpServiceConnectorTest.callDumpRecord();
    }
}
