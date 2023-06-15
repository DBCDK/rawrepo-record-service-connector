package dk.dbc.rawrepo.agency;

import dk.dbc.httpclient.FailSafeHttpClient;
import dk.dbc.httpclient.HttpGet;
import dk.dbc.httpclient.PathBuilder;
import dk.dbc.invariant.InvariantUtil;
import dk.dbc.rawrepo.dto.AgencyCollectionDTO;
import dk.dbc.rawrepo.dto.RecordIdCollectionDTO;
import dk.dbc.util.Stopwatch;
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.core.Response;
import net.jodah.failsafe.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class RecordAgencyServiceConnector {
    public enum TimingLogLevel {
        TRACE, DEBUG, INFO, WARN, ERROR
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(RecordAgencyServiceConnector.class);

    private static final String PATH_VARIABLE_AGENCY_ID = "agencyId";
    private static final String PATH_ALL_AGENCIES = "/api/v1/agencies";
    private static final String PATH_BIBLIOGRAPHIC_RECORD_IDS_FOR_AGENCY = String.format("api/v1/agency/{%s}/recordids",
            PATH_VARIABLE_AGENCY_ID);

    private static final RetryPolicy<Response> RETRY_POLICY = new RetryPolicy<Response>()
            .handle(ProcessingException.class)
            .handleResultIf(response -> response.getStatus() == 404
                    || response.getStatus() == 500
                    || response.getStatus() == 502)
            .withDelay(Duration.ofSeconds(10))
            .withMaxRetries(6);

    private final FailSafeHttpClient failSafeHttpClient;
    private final String baseUrl;
    private final RecordAgencyServiceConnector.LogLevelMethod logger;

    /**
     * Returns new instance with default retry policy
     *
     * @param httpClient web resources client
     * @param baseUrl    base URL for record service endpoint
     */
    public RecordAgencyServiceConnector(Client httpClient, String baseUrl) {
        this(FailSafeHttpClient.create(httpClient, RETRY_POLICY), baseUrl, RecordAgencyServiceConnector.TimingLogLevel.INFO);
    }

    /**
     * Returns new instance with default retry policy
     *
     * @param httpClient web resources client
     * @param baseUrl    base URL for record service endpoint
     * @param level      timings log level
     */
    public RecordAgencyServiceConnector(Client httpClient, String baseUrl, RecordAgencyServiceConnector.TimingLogLevel level) {
        this(FailSafeHttpClient.create(httpClient, RETRY_POLICY), baseUrl, level);
    }

    /**
     * Returns new instance with custom retry policy
     *
     * @param failSafeHttpClient web resources client with custom retry policy
     * @param baseUrl            base URL for record service endpoint
     */
    public RecordAgencyServiceConnector(FailSafeHttpClient failSafeHttpClient, String baseUrl) {
        this(failSafeHttpClient, baseUrl, RecordAgencyServiceConnector.TimingLogLevel.INFO);
    }

    /**
     * Returns new instance with custom retry policy
     *
     * @param failSafeHttpClient web resources client with custom retry policy
     * @param baseUrl            base URL for record service endpoint
     * @param level              timings log level
     */
    public RecordAgencyServiceConnector(FailSafeHttpClient failSafeHttpClient, String baseUrl, RecordAgencyServiceConnector.TimingLogLevel level) {
        this.failSafeHttpClient = InvariantUtil.checkNotNullOrThrow(
                failSafeHttpClient, "failSafeHttpClient");
        this.baseUrl = InvariantUtil.checkNotNullNotEmptyOrThrow(
                baseUrl, "baseUrl");
        switch (level) {
            case TRACE:
                logger = LOGGER::trace;
                break;
            case DEBUG:
                logger = LOGGER::debug;
                break;
            case INFO:
                logger = LOGGER::info;
                break;
            case WARN:
                logger = LOGGER::warn;
                break;
            case ERROR:
                logger = LOGGER::error;
                break;
            default:
                logger = LOGGER::info;
                break;
        }
    }

    public void close() {
        failSafeHttpClient.getClient().close();
    }

    public Integer[] getAllAgencies() throws RecordAgencyServiceConnectorException {
        final Stopwatch stopwatch = new Stopwatch();
        try {
            return sendRequest(PATH_ALL_AGENCIES, AgencyCollectionDTO.class).toArray();
        } finally {
            logger.log("getAllAgencies() took {} milliseconds",
                    stopwatch.getElapsedTime(TimeUnit.MILLISECONDS));
        }
    }

    public RecordIdCollectionDTO getBibliographicRecordIdsForAgencyId(String agencyId) throws RecordAgencyServiceConnectorException {
        final Stopwatch stopwatch = new Stopwatch();
        try {
            return sendRequest(PATH_BIBLIOGRAPHIC_RECORD_IDS_FOR_AGENCY, agencyId, RecordIdCollectionDTO.class);
        } finally {
            logger.log("getBibliographicRecordIdsForAgencyId({}) took {} milliseconds",
                    agencyId,
                    stopwatch.getElapsedTime(TimeUnit.MILLISECONDS));
        }
    }

    private <T> T sendRequest(String basePath, String agencyId, Class<T> type)
            throws RecordAgencyServiceConnectorException {
        InvariantUtil.checkNotNullNotEmptyOrThrow(agencyId, "agencyId");
        final PathBuilder path = new PathBuilder(basePath)
                .bind(PATH_VARIABLE_AGENCY_ID, agencyId);
        final HttpGet httpGet = new HttpGet(failSafeHttpClient)
                .withBaseUrl(baseUrl)
                .withPathElements(path.build());
        final Response response = httpGet.execute();
        assertResponseStatus(response, Response.Status.OK);
        return readResponseEntity(response, type);
    }

    private <T> T sendRequest(String basePath, Class<T> type)
            throws RecordAgencyServiceConnectorException {
        final PathBuilder path = new PathBuilder(basePath);
        final HttpGet httpGet = new HttpGet(failSafeHttpClient)
                .withBaseUrl(baseUrl)
                .withPathElements(path.build());
        final Response response = httpGet.execute();
        assertResponseStatus(response, Response.Status.OK);
        return readResponseEntity(response, type);
    }

    private <T> T readResponseEntity(Response response, Class<T> type)
            throws RecordAgencyServiceConnectorException {
        final T entity = response.readEntity(type);
        if (entity == null) {
            throw new RecordAgencyServiceConnectorException(
                    String.format("Record service returned with null-valued %s entity",
                            type.getName()));
        }
        return entity;
    }

    private void assertResponseStatus(Response response, Response.Status expectedStatus)
            throws RecordAgencyServiceConnectorUnexpectedStatusCodeException {
        final Response.Status actualStatus =
                Response.Status.fromStatusCode(response.getStatus());
        if (actualStatus != expectedStatus) {
            throw new RecordAgencyServiceConnectorUnexpectedStatusCodeException(
                    String.format("Record service returned with unexpected status code: %s",
                            actualStatus),
                    actualStatus.getStatusCode());
        }
    }

    @FunctionalInterface
    interface LogLevelMethod {
        void log(String format, Object... objs);
    }
}
