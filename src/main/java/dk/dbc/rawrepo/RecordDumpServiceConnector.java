/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GPLv3
 * See license text in LICENSE.txt or at https://opensource.dbc.dk/licenses/gpl-3.0/
 */

package dk.dbc.rawrepo;

import dk.dbc.httpclient.FailSafeHttpClient;
import dk.dbc.httpclient.HttpPost;
import dk.dbc.invariant.InvariantUtil;
import dk.dbc.jsonb.JSONBContext;
import dk.dbc.jsonb.JSONBException;
import dk.dbc.util.Stopwatch;
import net.jodah.failsafe.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class RecordDumpServiceConnector {
    public enum TimingLogLevel {
        TRACE, DEBUG, INFO, WARN, ERROR
    }

    private final JSONBContext jsonbContext = new JSONBContext();

    private static final Logger LOGGER = LoggerFactory.getLogger(RecordDumpServiceConnector.class);
    private static final String PATH_DUMP_AGENCY = "/api/v1/dump";
    private static final String PATH_DUMP_AGENCY_DRYRUN = "/api/v1/dump/dryrun";

    private static final RetryPolicy RETRY_POLICY = new RetryPolicy()
            .retryOn(Collections.singletonList(ProcessingException.class))
            .retryIf((Response response) -> response.getStatus() == 404)
            .withDelay(10, TimeUnit.SECONDS)
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

    public InputStream dumpAgenciesDryRun(Params params) throws RecordDumpServiceConnectorException {
        final Stopwatch stopwatch = new Stopwatch();
        try {
            return postRequest(PATH_DUMP_AGENCY_DRYRUN, params, InputStream.class);
        } finally {
            logger.log("dumpAgencies({}) took {} milliseconds",
                    params,
                    stopwatch.getElapsedTime(TimeUnit.MILLISECONDS));
        }
    }

    public InputStream dumpAgencies(Params params) throws RecordDumpServiceConnectorException {
        final Stopwatch stopwatch = new Stopwatch();
        try {
            return postRequest(PATH_DUMP_AGENCY, params, InputStream.class);
        } finally {
            logger.log("dumpAgencies({}) took {} milliseconds",
                    params,
                    stopwatch.getElapsedTime(TimeUnit.MILLISECONDS));
        }
    }

    private <S, T> T postRequest(String path, S data, Class<T> returnType) throws RecordDumpServiceConnectorException {
        logger.log("POST {} with data {}", path, data);
        final HttpPost httpPost = new HttpPost(failSafeHttpClient)
                .withBaseUrl(baseUrl)
                .withPathElements(path)
                .withJsonData(data)
                .withHeader("Accept", "text/plain")
                .withHeader("Content-type", "application/json");
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
                    final ParamsValidation validation = jsonbContext.unmarshall(entity, ParamsValidation.class);

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

    public static class Params extends HashMap<String, Object> {
        public enum Key {
            AGENCIES("agencies"),
            RECORD_STATUS("recordStatus"),
            RECORD_TYPE("recordType"),
            CREATED_FROM("createdFrom"),
            CREATED_TO("createdTo"),
            MODIFIED_FROM("modifiedFrom"),
            MODIFIED_TO("modifiedTo"),
            OUTPUT_FORMAT("outputFormat"),
            OUTPUT_ENCODING("outputEncoding");

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

        public Params withAgencies(List<Integer> agencies) {
            putOrRemoveOnNull(Key.AGENCIES, agencies);
            return this;
        }

        public Optional<List<String>> getAgencies() {
            return Optional.ofNullable((List<String>) this.get(Key.AGENCIES));
        }

        public Params withRecordStatus(RecordStatus recordStatus) {
            putOrRemoveOnNull(Key.RECORD_STATUS, recordStatus.toString());
            return this;
        }

        public Optional<RecordStatus> getRecordStatus() {
            return Optional.ofNullable((RecordStatus) this.get(Key.RECORD_STATUS));
        }

        public Params withRecordType(List<RecordType> recordTypes) {
            putOrRemoveOnNull(Key.RECORD_TYPE, recordTypes.stream()
                    .map(Enum::toString)
                    .collect(Collectors.toList()));
            return this;
        }

        public Optional<RecordType> getRecordType() {
            return Optional.ofNullable((RecordType) this.get(Key.RECORD_TYPE));
        }

        public Params withCreatedFrom(String date) {
            putOrRemoveOnNull(Key.CREATED_FROM, date);
            return this;
        }

        public Optional<String> getCreatedFrom() {
            return Optional.ofNullable((String) this.get(Key.CREATED_FROM));
        }

        public Params withCreatedTo(String date) {
            putOrRemoveOnNull(Key.CREATED_TO, date);
            return this;
        }

        public Optional<String> getCreatedTo() {
            return Optional.ofNullable((String) this.get(Key.CREATED_TO));
        }

        public Params withModifiedFrom(String date) {
            putOrRemoveOnNull(Key.MODIFIED_FROM, date);
            return this;
        }

        public Optional<String> getModifiedFrom() {
            return Optional.ofNullable((String) this.get(Key.MODIFIED_FROM));
        }

        public Params withModifiedTo(String date) {
            putOrRemoveOnNull(Key.MODIFIED_TO, date);
            return this;
        }

        public Optional<String> getModifiedTo() {
            return Optional.ofNullable((String) this.get(Key.MODIFIED_TO));
        }

        public Params withOutputFormat(OutputFormat outputFormat) {
            putOrRemoveOnNull(Key.OUTPUT_FORMAT, outputFormat.toString());
            return this;
        }

        public Optional<OutputFormat> getOutputFormat() {
            return Optional.ofNullable((OutputFormat) this.get(Key.OUTPUT_FORMAT));
        }

        public Params withOutputEncoding(String outputEncoding) {
            putOrRemoveOnNull(Key.OUTPUT_ENCODING, outputEncoding);
            return this;
        }

        public Optional<String> getOutputEncoding() {
            return Optional.ofNullable((String) this.get(Key.OUTPUT_ENCODING));
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
