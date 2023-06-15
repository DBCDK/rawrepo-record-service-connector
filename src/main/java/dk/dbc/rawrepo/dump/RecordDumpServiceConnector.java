package dk.dbc.rawrepo.dump;

import dk.dbc.commons.jsonb.JSONBContext;
import dk.dbc.commons.jsonb.JSONBException;
import dk.dbc.httpclient.FailSafeHttpClient;
import dk.dbc.httpclient.HttpPost;
import dk.dbc.invariant.InvariantUtil;
import dk.dbc.rawrepo.dto.ParamsValidationDTO;
import dk.dbc.util.Stopwatch;
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.core.Response;
import net.jodah.failsafe.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static jakarta.ws.rs.core.MediaType.TEXT_PLAIN;

public class RecordDumpServiceConnector {
    public enum TimingLogLevel {
        TRACE, DEBUG, INFO, WARN, ERROR
    }

    private final JSONBContext jsonbContext = new JSONBContext();

    private static final Logger LOGGER = LoggerFactory.getLogger(RecordDumpServiceConnector.class);
    private static final String PATH_DUMP_AGENCY = "/api/v1/dump";
    private static final String PATH_DUMP_RECORD = "/api/v1/dump/record";
    private static final String PATH_DUMP_AGENCY_DRYRUN = "/api/v1/dump/dryrun";

    private static final RetryPolicy<Response> RETRY_POLICY = new RetryPolicy<Response>()
            .handle(ProcessingException.class)
            .handleResultIf(response -> response.getStatus() == 404)
            .withDelay(Duration.ofSeconds(10))
            .withMaxRetries(1);

    private final FailSafeHttpClient failSafeHttpClient;
    private final String baseUrl;
    private final RecordDumpServiceConnector.LogLevelMethod logger;

    /**
     * Returns new instance with default retry policy
     *
     * @param httpClient web resources client
     * @param baseUrl    base URL for record service endpoint
     */
    public RecordDumpServiceConnector(Client httpClient, String baseUrl) {
        this(FailSafeHttpClient.create(httpClient, RETRY_POLICY), baseUrl, RecordDumpServiceConnector.TimingLogLevel.INFO);
    }

    /**
     * Returns new instance with default retry policy
     *
     * @param httpClient web resources client
     * @param baseUrl    base URL for record service endpoint
     * @param level      timings log level
     */
    public RecordDumpServiceConnector(Client httpClient, String baseUrl, RecordDumpServiceConnector.TimingLogLevel level) {
        this(FailSafeHttpClient.create(httpClient, RETRY_POLICY), baseUrl, level);
    }

    /**
     * Returns new instance with custom retry policy
     *
     * @param failSafeHttpClient web resources client with custom retry policy
     * @param baseUrl            base URL for record service endpoint
     */
    public RecordDumpServiceConnector(FailSafeHttpClient failSafeHttpClient, String baseUrl) {
        this(failSafeHttpClient, baseUrl, RecordDumpServiceConnector.TimingLogLevel.INFO);
    }

    /**
     * Returns new instance with custom retry policy
     *
     * @param failSafeHttpClient web resources client with custom retry policy
     * @param baseUrl            base URL for record service endpoint
     * @param level              timings log level
     */
    public RecordDumpServiceConnector(FailSafeHttpClient failSafeHttpClient, String baseUrl, RecordDumpServiceConnector.TimingLogLevel level) {
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

    public InputStream dumpAgenciesDryRun(AgencyParams params) throws RecordDumpServiceConnectorException {
        final Stopwatch stopwatch = new Stopwatch();
        try {
            return postRequestAgency(PATH_DUMP_AGENCY_DRYRUN, params, InputStream.class);
        } finally {
            logger.log("dumpAgencies({}) took {} milliseconds",
                    params,
                    stopwatch.getElapsedTime(TimeUnit.MILLISECONDS));
        }
    }

    public InputStream dumpAgencies(AgencyParams params) throws RecordDumpServiceConnectorException {
        final Stopwatch stopwatch = new Stopwatch();
        try {
            return postRequestAgency(PATH_DUMP_AGENCY, params, InputStream.class);
        } finally {
            logger.log("dumpAgencies({}) took {} milliseconds",
                    params,
                    stopwatch.getElapsedTime(TimeUnit.MILLISECONDS));
        }
    }

    public InputStream dumpRecords(RecordParams params, String body) throws RecordDumpServiceConnectorException {
        final Stopwatch stopwatch = new Stopwatch();
        try {
            return postRequestRecord(PATH_DUMP_RECORD, body, params, InputStream.class);
        } finally {
            logger.log("dumpRecords({}) took {} milliseconds",
                    params,
                    stopwatch.getElapsedTime(TimeUnit.MILLISECONDS));
        }

    }

    private <S, T> T postRequestAgency(String path, S data, Class<T> returnType) throws RecordDumpServiceConnectorException {
        logger.log("POST {} with data {}", path, data);
        final HttpPost httpPost = new HttpPost(failSafeHttpClient)
                .withBaseUrl(baseUrl)
                .withPathElements(path)
                .withJsonData(data)
                .withHeader("Accept", TEXT_PLAIN)
                .withHeader("Content-type", APPLICATION_JSON);
        final Response response = httpPost.execute();
        assertResponseStatus(response, Response.Status.OK);
        return readResponseEntity(response, returnType);
    }

    private <S, T> T postRequestRecord(String basePath, String body, RecordParams params, Class<T> returnType) throws RecordDumpServiceConnectorException {
        logger.log("POST {} with data \n{}", basePath, body);
        final HttpPost httpPost = new HttpPost(failSafeHttpClient)
                .withBaseUrl(baseUrl)
                .withPathElements(basePath)
                .withData(body, TEXT_PLAIN)
                .withHeader("Accept", TEXT_PLAIN);
        if (params != null) {
            for (Map.Entry<String, Object> param : params.entrySet()) {
                httpPost.withQueryParameter(param.getKey(), param.getValue());
            }
        }
        final Response response = httpPost.execute();
        assertResponseStatus(response, Response.Status.OK);
        return readResponseEntity(response, returnType);
    }

    private <T> T readResponseEntity(Response response, Class<T> type)
            throws RecordDumpServiceConnectorException {
        final T entity = response.readEntity(type);
        if (entity == null) {
            throw new RecordDumpServiceConnectorException(
                    String.format("Record service returned with null-valued %s entity",
                            type.getName()));
        }
        return entity;
    }

    private void assertResponseStatus(Response response, Response.Status expectedStatus)
            throws RecordDumpServiceConnectorUnexpectedStatusCodeException {
        final Response.Status actualStatus =
                Response.Status.fromStatusCode(response.getStatus());
        if (actualStatus != expectedStatus) {
            if (actualStatus == Response.Status.BAD_REQUEST) {
                try {
                    final String entity = response.readEntity(String.class);
                    final ParamsValidationDTO validation = jsonbContext.unmarshall(entity, ParamsValidationDTO.class);

                    throw new RecordDumpServiceConnectorUnexpectedStatusCodeValidationException(validation, actualStatus.getStatusCode());
                } catch (JSONBException e) {
                    throw new RecordDumpServiceConnectorUnexpectedStatusCodeException(
                            String.format("Got error code %s but while reading the message got error %s",
                                    actualStatus, e.getMessage()),
                            actualStatus.getStatusCode());
                }
            } else {
                // Special case handling for MarcReaderException
                final String marcReaderException = "dk.dbc.marc.reader.MarcReaderException: ";
                final String message = response.readEntity(String.class);
                if (message.contains(marcReaderException)) {
                    throw new RecordDumpServiceConnectorUnexpectedStatusCodeException(
                            String.format("Error from Record service: %s",
                                    message.substring(message.indexOf(marcReaderException) + marcReaderException.length())),
                            actualStatus.getStatusCode());
                } else {
                    throw new RecordDumpServiceConnectorUnexpectedStatusCodeException(
                            String.format("Record service returned with unexpected status code: %s",
                                    actualStatus),
                            actualStatus.getStatusCode());
                }
            }
        }
    }

    public void close() {
        failSafeHttpClient.getClient().close();
    }

    @FunctionalInterface
    interface LogLevelMethod {
        void log(String format, Object... objs);
    }

    public static class AgencyParams extends HashMap<String, Object> {
        public enum Key {
            AGENCIES("agencies"),
            RECORD_STATUS("recordStatus"),
            RECORD_TYPE("recordType"),
            CREATED_FROM("createdFrom"),
            CREATED_TO("createdTo"),
            MODIFIED_FROM("modifiedFrom"),
            MODIFIED_TO("modifiedTo"),
            OUTPUT_FORMAT("outputFormat"),
            OUTPUT_ENCODING("outputEncoding"),
            MODE("mode");

            private final String keyName;

            Key(String keyName) {
                this.keyName = keyName;
            }

            public String getKeyName() {
                return keyName;
            }
        }

        public enum RecordType {
            LOCAL, ENRICHMENT, HOLDINGS;

            public static List<String> list() {
                List<String> res = new ArrayList<>();

                for (RecordType value : RecordType.values()) {
                    res.add(value.name());
                }

                return res;
            }
        }

        public enum RecordStatus {
            ACTIVE, ALL, DELETED;

            public static List<String> list() {
                List<String> res = new ArrayList<>();

                for (RecordStatus value : RecordStatus.values()) {
                    res.add(value.name());
                }

                return res;
            }
        }

        public enum OutputFormat {
            LINE, XML, JSON, ISO, LINE_XML;

            public static List<String> list() {
                List<String> res = new ArrayList<>();

                for (OutputFormat value : OutputFormat.values()) {
                    res.add(value.name());
                }

                return res;
            }
        }

        public enum Mode {
            RAW, MERGED, EXPANDED;

            public static List<String> list() {
                List<String> res = new ArrayList<>();

                for (Mode value : Mode.values()) {
                    res.add(value.name());
                }

                return res;
            }
        }

        public AgencyParams withAgencies(List<Integer> agencies) {
            putOrRemoveOnNull(Key.AGENCIES, agencies);
            return this;
        }

        public Optional<List<Integer>> getAgencies() {
            return Optional.ofNullable((List<Integer>) this.get(Key.AGENCIES));
        }

        public AgencyParams withRecordStatus(RecordStatus recordStatus) {
            putOrRemoveOnNull(Key.RECORD_STATUS, recordStatus.toString());
            return this;
        }

        public Optional<RecordStatus> getRecordStatus() {
            return Optional.ofNullable((RecordStatus) this.get(Key.RECORD_STATUS));
        }

        public AgencyParams withRecordType(List<RecordType> recordTypes) {
            putOrRemoveOnNull(Key.RECORD_TYPE, recordTypes.stream()
                    .map(Enum::toString)
                    .collect(Collectors.toList()));
            return this;
        }

        public Optional<RecordType> getRecordType() {
            return Optional.ofNullable((RecordType) this.get(Key.RECORD_TYPE));
        }

        public AgencyParams withCreatedFrom(String date) {
            putOrRemoveOnNull(Key.CREATED_FROM, date);
            return this;
        }

        public Optional<String> getCreatedFrom() {
            return Optional.ofNullable((String) this.get(Key.CREATED_FROM));
        }

        public AgencyParams withCreatedTo(String date) {
            putOrRemoveOnNull(Key.CREATED_TO, date);
            return this;
        }

        public Optional<String> getCreatedTo() {
            return Optional.ofNullable((String) this.get(Key.CREATED_TO));
        }

        public AgencyParams withModifiedFrom(String date) {
            putOrRemoveOnNull(Key.MODIFIED_FROM, date);
            return this;
        }

        public Optional<String> getModifiedFrom() {
            return Optional.ofNullable((String) this.get(Key.MODIFIED_FROM));
        }

        public AgencyParams withModifiedTo(String date) {
            putOrRemoveOnNull(Key.MODIFIED_TO, date);
            return this;
        }

        public Optional<String> getModifiedTo() {
            return Optional.ofNullable((String) this.get(Key.MODIFIED_TO));
        }

        public AgencyParams withOutputFormat(OutputFormat outputFormat) {
            putOrRemoveOnNull(Key.OUTPUT_FORMAT, outputFormat.toString());
            return this;
        }

        public Optional<OutputFormat> getOutputFormat() {
            return Optional.ofNullable((OutputFormat) this.get(Key.OUTPUT_FORMAT));
        }

        public AgencyParams withOutputEncoding(String outputEncoding) {
            putOrRemoveOnNull(Key.OUTPUT_ENCODING, outputEncoding);
            return this;
        }

        public Optional<String> getOutputEncoding() {
            return Optional.ofNullable((String) this.get(Key.OUTPUT_ENCODING));
        }

        public AgencyParams withMode(Mode mode) {
            putOrRemoveOnNull(Key.MODE, mode.toString());
            return this;
        }

        public Optional<Mode> getMode() {
            return Optional.ofNullable((Mode) this.get(Key.MODE));
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

    public static class RecordParams extends HashMap<String, Object> {
        public enum Key {
            OUTPUT_FORMAT("output-format"),
            OUTPUT_ENCODING("output-encoding"),
            MODE("mode");

            private final String keyName;

            Key(String keyName) {
                this.keyName = keyName;
            }

            public String getKeyName() {
                return keyName;
            }
        }

        public enum OutputFormat {
            LINE, XML, JSON, ISO, LINE_XML;

            public static List<String> list() {
                List<String> res = new ArrayList<>();

                for (OutputFormat value : OutputFormat.values()) {
                    res.add(value.name());
                }

                return res;
            }
        }

        public RecordParams withOutputFormat(OutputFormat outputFormat) {
            putOrRemoveOnNull(Key.OUTPUT_FORMAT, outputFormat.toString());
            return this;
        }

        public Optional<OutputFormat> getOutputFormat() {
            return Optional.ofNullable((OutputFormat) this.get(Key.OUTPUT_FORMAT));
        }

        public RecordParams withOutputEncoding(String outputEncoding) {
            putOrRemoveOnNull(Key.OUTPUT_ENCODING, outputEncoding);
            return this;
        }

        public Optional<String> getOutputEncoding() {
            return Optional.ofNullable((String) this.get(Key.OUTPUT_ENCODING));
        }

        public RecordParams withMode(AgencyParams.Mode mode) {
            putOrRemoveOnNull(Key.MODE, mode.toString());
            return this;
        }

        public Optional<AgencyParams.Mode> getMode() {
            return Optional.ofNullable((AgencyParams.Mode) this.get(AgencyParams.Key.MODE));
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
}
