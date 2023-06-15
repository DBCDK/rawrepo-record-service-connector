package dk.dbc.rawrepo;

import com.github.tomakehurst.wiremock.WireMockServer;
import dk.dbc.httpclient.HttpClient;
import dk.dbc.rawrepo.dump.RecordDumpServiceConnector;
import dk.dbc.rawrepo.dump.RecordDumpServiceConnectorException;
import jakarta.ws.rs.client.Client;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

import static com.github.tomakehurst.wiremock.client.WireMock.configureFor;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class RecordDumpServiceConnectorTest {
    private static WireMockServer wireMockServer;
    private static String wireMockHost;

    private final static Client CLIENT = HttpClient.newClient(new ClientConfig()
            .register(new JacksonFeature()));
    private static RecordDumpServiceConnector connector;

    private String loadFileContent(String filename) throws FileNotFoundException {
        final File file = new File("src/test/resources/" + filename);
        final FileInputStream fstream = new FileInputStream(file);

        return new BufferedReader(new InputStreamReader(fstream)).lines().collect(Collectors.joining("\n"));
    }

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
        connector = new RecordDumpServiceConnector(CLIENT, wireMockHost, RecordDumpServiceConnector.TimingLogLevel.INFO);
    }

    @AfterAll
    static void stopWireMockServer() {
        wireMockServer.stop();
    }

    @Test
    void params() {
        final RecordDumpServiceConnector.AgencyParams params = new RecordDumpServiceConnector.AgencyParams();
        params.withAgencies(Collections.singletonList(710100));
        assertThat("param set",
                params.getAgencies().isPresent(), is(true));
        assertThat("param value",
                params.getAgencies().get(), is(Collections.singletonList(710100)));
        params.withAgencies(null);
        assertThat("param removed on null",
                params.getAgencies().isPresent(), is(false));
    }

    @Test
    void callDumpAgencyDryRun() throws RecordDumpServiceConnectorException, IOException {
        RecordDumpServiceConnector.AgencyParams params = new RecordDumpServiceConnector.AgencyParams()
                .withAgencies(Collections.singletonList(710100))
                .withRecordType(Arrays.asList(RecordDumpServiceConnector.AgencyParams.RecordType.LOCAL,
                        RecordDumpServiceConnector.AgencyParams.RecordType.ENRICHMENT,
                        RecordDumpServiceConnector.AgencyParams.RecordType.HOLDINGS))
                .withRecordStatus(RecordDumpServiceConnector.AgencyParams.RecordStatus.ACTIVE);

        try (InputStream inputStream = connector.dumpAgenciesDryRun(params)) {
            String result = new BufferedReader(new InputStreamReader(inputStream))
                    .lines().collect(Collectors.joining("\n"));

            assertThat(result, is("1"));
        }
    }

    @Test
    void callDumpAgency() throws RecordDumpServiceConnectorException, IOException {
        RecordDumpServiceConnector.AgencyParams params = new RecordDumpServiceConnector.AgencyParams()
                .withAgencies(Collections.singletonList(710100))
                .withRecordType(Arrays.asList(RecordDumpServiceConnector.AgencyParams.RecordType.LOCAL,
                        RecordDumpServiceConnector.AgencyParams.RecordType.ENRICHMENT,
                        RecordDumpServiceConnector.AgencyParams.RecordType.HOLDINGS))
                .withRecordStatus(RecordDumpServiceConnector.AgencyParams.RecordStatus.ACTIVE);

        String expected = "001 00 *a01118633*b710100*c20190305091812*d19760504*fa\n" +
                "004 00 *rn*ae\n" +
                "008 00 *tm*uf*a1972*bde*dy*lger*v1\n" +
                "009 00 *aa*gxx\n" +
                "021 00 *a3-462-00858-7\n" +
                "100 00 *aUngers*hLiselotte\n" +
                "245 00 *aKommunen in der Neuen Welt 1740-1971*evon Liselotte und O.M. Ungers\n" +
                "260 00 *aKöln*bKiepenheuer & Witsch*c1972\n" +
                "300 00 *a102 sider, 32 tavler*d1kort\n" +
                "440 00 *aPocket*v32\n" +
                "532 00 *aBibliographie: side 101-102\n" +
                "652 00 *m30.17\n" +
                "652 00 *p32.2863\n" +
                "652 00 *p98.63\n" +
                "700 00 *aUngers*hO. M.\n" +
                "996 00 *aDBC\n" +
                "$";

        try (InputStream inputStream = connector.dumpAgencies(params)) {
            String result = new BufferedReader(new InputStreamReader(inputStream))
                    .lines().collect(Collectors.joining("\n"));

            assertThat(result, is(expected));
        }
    }

    @Test
    void callDumpRecord() throws RecordDumpServiceConnectorException, IOException {
        RecordDumpServiceConnector.RecordParams params = new RecordDumpServiceConnector.RecordParams();
        params.withOutputFormat(RecordDumpServiceConnector.RecordParams.OutputFormat.LINE);
        params.withOutputEncoding("UTF-8");

        String body = "22058037:735000\n" +
                "22058037:870970\n" +
                "52722489:870970";

        try (InputStream inputStream = connector.dumpRecords(params, body)) {
            String result = new BufferedReader(new InputStreamReader(inputStream))
                    .lines().collect(Collectors.joining("\n"));

            assertThat(result, is(loadFileContent("dump-record-expected.txt")));
        }
    }
}
