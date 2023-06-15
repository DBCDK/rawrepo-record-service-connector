package dk.dbc.rawrepo.queue;

import dk.dbc.httpclient.FailSafeHttpClient;
import dk.dbc.httpclient.HttpGet;
import dk.dbc.httpclient.HttpPost;
import dk.dbc.httpclient.PathBuilder;
import dk.dbc.invariant.InvariantUtil;
import dk.dbc.rawrepo.dto.EnqueueAgencyResponseDTO;
import dk.dbc.rawrepo.dto.EnqueueResultCollectionDTO;
import dk.dbc.rawrepo.dto.QueueProviderCollectionDTO;
import dk.dbc.rawrepo.dto.QueueRuleCollectionDTO;
import dk.dbc.rawrepo.dto.QueueStatDTO;
import dk.dbc.rawrepo.dto.QueueWorkerCollectionDTO;
import dk.dbc.util.Stopwatch;
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.core.Response;
import net.jodah.failsafe.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class QueueServiceConnector {
    public enum TimingLogLevel {
        TRACE, DEBUG, INFO, WARN, ERROR
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(QueueServiceConnector.class);

    private static final String PATH_ALL_QUEUE_RULES = "/api/v1/queue/rules";
    private static final String PATH_ALL_QUEUE_PROVIDERS = "/api/v1/queue/providers";
    private static final String PATH_ALL_QUEUE_WORKERS = "/api/v1/queue/workers";

    private static final String PATH_ALL_QUEUE_WORKER_STATS = "/api/v1/queue/stats/workers";
    private static final String PATH_ALL_QUEUE_AGENCY_STATS = "/api/v1/queue/stats/agency";

    private static final String PATH_VARIABLE_AGENCY_ID = "agencyid";
    private static final String PATH_VARIABLE_BIBLIOGRAPHIC_RECORD_ID = "bibliographicrecordid";
    private static final String PATH_VARIABLE_WORKER = "worker";
    private static final String PATH_VARIABLE_PROVIDER = "provider";

    private static final String PATH_ENQUEUE_AGENCY = String.format("/api/v1/queue/{%s}/{%s}",
            PATH_VARIABLE_AGENCY_ID, PATH_VARIABLE_WORKER);
    private static final String PATH_ENQUEUE_RECORD = String.format("/api/v1/queue/{%s}/{%s}/{%s}",
            PATH_VARIABLE_AGENCY_ID, PATH_VARIABLE_BIBLIOGRAPHIC_RECORD_ID, PATH_VARIABLE_PROVIDER);

    private static final RetryPolicy<Response> RETRY_POLICY = new RetryPolicy<Response>()
            .handle(ProcessingException.class)
            .handleResultIf(response -> response.getStatus() == 404
                    || response.getStatus() == 500
                    || response.getStatus() == 502)
            .withDelay(Duration.ofSeconds(10))
            .withMaxRetries(6);

    private final FailSafeHttpClient failSafeHttpClient;
    private final String baseUrl;
    private final QueueServiceConnector.LogLevelMethod logger;

    /**
     * Returns new instance with default retry policy
     *
     * @param httpClient web resources client
     * @param baseUrl    base URL for record service endpoint
     */
    public QueueServiceConnector(Client httpClient, String baseUrl) {
        this(FailSafeHttpClient.create(httpClient, RETRY_POLICY), baseUrl, QueueServiceConnector.TimingLogLevel.INFO);
    }

    /**
     * Returns new instance with default retry policy
     *
     * @param httpClient web resources client
     * @param baseUrl    base URL for record service endpoint
     * @param level      timings log level
     */
    public QueueServiceConnector(Client httpClient, String baseUrl, QueueServiceConnector.TimingLogLevel level) {
        this(FailSafeHttpClient.create(httpClient, RETRY_POLICY), baseUrl, level);
    }

    /**
     * Returns new instance with custom retry policy
     *
     * @param failSafeHttpClient web resources client with custom retry policy
     * @param baseUrl            base URL for record service endpoint
     */
    public QueueServiceConnector(FailSafeHttpClient failSafeHttpClient, String baseUrl) {
        this(failSafeHttpClient, baseUrl, QueueServiceConnector.TimingLogLevel.INFO);
    }

    /**
     * Returns new instance with custom retry policy
     *
     * @param failSafeHttpClient web resources client with custom retry policy
     * @param baseUrl            base URL for record service endpoint
     * @param level              timings log level
     */
    public QueueServiceConnector(FailSafeHttpClient failSafeHttpClient, String baseUrl, QueueServiceConnector.TimingLogLevel level) {
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

    public QueueRuleCollectionDTO getQueueRules() throws QueueServiceConnectorException {
        final Stopwatch stopwatch = new Stopwatch();
        try {
            return sendRequest(PATH_ALL_QUEUE_RULES, QueueRuleCollectionDTO.class);
        } finally {
            logger.log("getQueueRules() took {} milliseconds",
                    stopwatch.getElapsedTime(TimeUnit.MILLISECONDS));
        }
    }

    public QueueProviderCollectionDTO getQueueProviders() throws QueueServiceConnectorException {
        final Stopwatch stopwatch = new Stopwatch();
        try {
            return sendRequest(PATH_ALL_QUEUE_PROVIDERS, QueueProviderCollectionDTO.class);
        } finally {
            logger.log("getQueueProviders() took {} milliseconds",
                    stopwatch.getElapsedTime(TimeUnit.MILLISECONDS));
        }
    }

    public QueueWorkerCollectionDTO getQueueWorkers() throws QueueServiceConnectorException {
        final Stopwatch stopwatch = new Stopwatch();
        try {
            return sendRequest(PATH_ALL_QUEUE_WORKERS, QueueWorkerCollectionDTO.class);
        } finally {
            logger.log("getQueueWorkers() took {} milliseconds",
                    stopwatch.getElapsedTime(TimeUnit.MILLISECONDS));
        }
    }

    public QueueStatDTO getQueueWorkerStats() throws QueueServiceConnectorException {
        final Stopwatch stopwatch = new Stopwatch();
        try {
            return sendRequest(PATH_ALL_QUEUE_WORKER_STATS, QueueStatDTO.class);
        } finally {
            logger.log("getQueueWorkerStats() took {} milliseconds",
                    stopwatch.getElapsedTime(TimeUnit.MILLISECONDS));
        }
    }

    public QueueStatDTO getQueueAgencyStats() throws QueueServiceConnectorException {
        final Stopwatch stopwatch = new Stopwatch();
        try {
            return sendRequest(PATH_ALL_QUEUE_AGENCY_STATS, QueueStatDTO.class);
        } finally {
            logger.log("getQueueAgencyStats() took {} milliseconds",
                    stopwatch.getElapsedTime(TimeUnit.MILLISECONDS));
        }
    }

    public EnqueueAgencyResponseDTO enqueueAgency(int agencyId, String worker)
            throws QueueServiceConnectorException {
        return enqueueAgency(agencyId, worker, null);
    }

    public EnqueueAgencyResponseDTO enqueueAgency(int agencyId, String worker, EnqueueParams params)
            throws QueueServiceConnectorException {
        final Stopwatch stopwatch = new Stopwatch();
        try {
            return postEnqueueAgencyRequest(PATH_ENQUEUE_AGENCY, agencyId, worker, params, EnqueueAgencyResponseDTO.class);
        } finally {
            logger.log("enqueueAgency() took {} milliseconds",
                    stopwatch.getElapsedTime(TimeUnit.MILLISECONDS));
        }
    }

    public EnqueueResultCollectionDTO enqueueRecord(int agencyId, String bibliographicRecordId, String provider)
            throws QueueServiceConnectorException {
        return enqueueRecord(agencyId, bibliographicRecordId, provider, null);
    }

    public EnqueueResultCollectionDTO enqueueRecord(int agencyId, String bibliographicRecordId, String provider, EnqueueParams params)
            throws QueueServiceConnectorException {
        final Stopwatch stopwatch = new Stopwatch();
        try {
            return postEnqueueRecordRequest(PATH_ENQUEUE_RECORD, agencyId, bibliographicRecordId, provider, params, EnqueueResultCollectionDTO.class);
        } finally {
            logger.log("enqueueRecord() took {} milliseconds",
                    stopwatch.getElapsedTime(TimeUnit.MILLISECONDS));
        }
    }

    private <S, T> T postEnqueueAgencyRequest(String basePath,
                                              int agencyId,
                                              String worker,
                                              EnqueueParams params,
                                              Class<T> type) throws QueueServiceConnectorException {
        final PathBuilder path = new PathBuilder(basePath)
                .bind(PATH_VARIABLE_AGENCY_ID, agencyId)
                .bind(PATH_VARIABLE_WORKER, worker);
        final HttpPost httpPost = new HttpPost(failSafeHttpClient)
                .withBaseUrl(baseUrl)
                .withPathElements(path.build())
                .withHeader("Accept", "application/json");
        if (params != null) {
            for (Map.Entry<String, Object> param : params.entrySet()) {
                httpPost.withQueryParameter(param.getKey(), param.getValue());
            }
        }
        final Response response = httpPost.execute();
        assertResponseStatus(response, Response.Status.OK);
        return readResponseEntity(response, type);
    }

    private <S, T> T postEnqueueRecordRequest(String basePath,
                                              int agencyId,
                                              String bibliographicRecordId,
                                              String provider,
                                              EnqueueParams params,
                                              Class<T> type) throws QueueServiceConnectorException {
        final PathBuilder path = new PathBuilder(basePath)
                .bind(PATH_VARIABLE_AGENCY_ID, agencyId)
                .bind(PATH_VARIABLE_BIBLIOGRAPHIC_RECORD_ID, bibliographicRecordId)
                .bind(PATH_VARIABLE_PROVIDER, provider);
        final HttpPost httpPost = new HttpPost(failSafeHttpClient)
                .withBaseUrl(baseUrl)
                .withPathElements(path.build())
                .withHeader("Accept", "application/json");
        if (params != null) {
            for (Map.Entry<String, Object> param : params.entrySet()) {
                httpPost.withQueryParameter(param.getKey(), param.getValue());
            }
        }
        final Response response = httpPost.execute();
        assertResponseStatus(response, Response.Status.OK);
        return readResponseEntity(response, type);
    }

    private <T> T sendRequest(String basePath, Class<T> type)
            throws QueueServiceConnectorException {
        final PathBuilder path = new PathBuilder(basePath);
        final HttpGet httpGet = new HttpGet(failSafeHttpClient)
                .withBaseUrl(baseUrl)
                .withPathElements(path.build());
        final Response response = httpGet.execute();
        assertResponseStatus(response, Response.Status.OK);
        return readResponseEntity(response, type);
    }

    private <T> T readResponseEntity(Response response, Class<T> type)
            throws QueueServiceConnectorException {
        final T entity = response.readEntity(type);
        if (entity == null) {
            throw new QueueServiceConnectorException(
                    String.format("Record service returned with null-valued %s entity",
                            type.getName()));
        }
        return entity;
    }

    private void assertResponseStatus(Response response, Response.Status expectedStatus)
            throws QueueServiceConnectorUnexpectedStatusCodeException {
        final Response.Status actualStatus =
                Response.Status.fromStatusCode(response.getStatus());
        if (actualStatus != expectedStatus) {
            throw new QueueServiceConnectorUnexpectedStatusCodeException(
                    String.format("Record service returned with unexpected status code: %s",
                            actualStatus),
                    actualStatus.getStatusCode());
        }
    }

    public static class EnqueueParams extends HashMap<String, Object> {
        public enum Key {
            ENQUEUE_AS("enqueue-as"),
            PRIORITY("priority"),
            CHANGED("changed"),
            LEAF("leaf");

            private final String keyName;

            Key(String keyName) {
                this.keyName = keyName;
            }

            public String getKeyName() {
                return keyName;
            }
        }

        public EnqueueParams withEnqueueAs(Integer enqueueAs) {
            putOrRemoveOnNull(Key.ENQUEUE_AS, enqueueAs);
            return this;
        }

        public Optional<Integer> getForCorepo() {
            return Optional.ofNullable((Integer) this.get(Key.ENQUEUE_AS));
        }

        public EnqueueParams withPriority(Integer enqueueAs) {
            putOrRemoveOnNull(Key.PRIORITY, enqueueAs);
            return this;
        }

        public Optional<Integer> getPriority() {
            return Optional.ofNullable((Integer) this.get(Key.PRIORITY));
        }

        public EnqueueParams withChanged(Boolean changed) {
            putOrRemoveOnNull(Key.CHANGED, changed);
            return this;
        }

        public Optional<Boolean> getChanged() {
            return Optional.ofNullable((Boolean) this.get(Key.CHANGED));
        }

        public EnqueueParams withLeaf(Boolean changed) {
            putOrRemoveOnNull(Key.LEAF, changed);
            return this;
        }

        public Optional<Boolean> getLeaf() {
            return Optional.ofNullable((Boolean) this.get(Key.LEAF));
        }

        private void putOrRemoveOnNull(Key param, Object value) {
            if (value == null) {
                this.remove(param.keyName);
            } else {
                this.put(param.keyName, value);
            }
        }

        private Object get(Key param) {
            return get(param.keyName);
        }
    }

    @FunctionalInterface
    interface LogLevelMethod {
        void log(String format, Object... objs);
    }
}
