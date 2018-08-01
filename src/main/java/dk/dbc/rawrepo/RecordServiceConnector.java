/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GPLv3
 * See license text in LICENSE.txt or at https://opensource.dbc.dk/licenses/gpl-3.0/
 */

package dk.dbc.rawrepo;

import dk.dbc.httpclient.FailSafeHttpClient;
import dk.dbc.httpclient.HttpGet;
import dk.dbc.httpclient.PathBuilder;
import dk.dbc.invariant.InvariantUtil;
import dk.dbc.util.Stopwatch;
import net.jodah.failsafe.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * RecordServiceConnector - rawrepo record service client
 * <p>
 * To use this class, you construct an instance, specifying a web resources client as well as
 * a base URL for the record service endpoint you will be communicating with.
 * </p>
 * <p>
 * This class is thread safe, as long as the given web resources client remains thread safe.
 * </p>
 * <p>
 * Service home: https://github.com/DBCDK/rawrepo-record-service
 * </p>
 */
public class RecordServiceConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordServiceConnector.class);
    private static final String PATH_VARIABLE_AGENCY_ID = "agencyId";
    private static final String PATH_VARIABLE_BIBLIOGRAPHIC_RECORD_ID = "bibliographicRecordId";
    private static final String PATH_RECORD_EXISTS = String.format("/api/v1/record/{%s}/{%s}/exists",
            PATH_VARIABLE_AGENCY_ID, PATH_VARIABLE_BIBLIOGRAPHIC_RECORD_ID);

    private static final RetryPolicy RETRY_POLICY = new RetryPolicy()
            .retryOn(Collections.singletonList(ProcessingException.class))
            .retryIf((Response response) -> response.getStatus() == 404
                    || response.getStatus() == 500
                    || response.getStatus() == 502)
            .withDelay(10, TimeUnit.SECONDS)
            .withMaxRetries(6);

    private final FailSafeHttpClient failSafeHttpClient;
    private final String baseUrl;

    /**
     * Returns new instance with default retry policy
     * @param httpClient web resources client
     * @param baseUrl base URL for record service endpoint
     */
    public RecordServiceConnector(Client httpClient, String baseUrl) {
        this(FailSafeHttpClient.create(httpClient, RETRY_POLICY), baseUrl);
    }

    /**
     * Returns new instance with custom retry policy
     * @param failSafeHttpClient web resources client with custom retry policy
     * @param baseUrl base URL for record service endpoint
     */
    public RecordServiceConnector(FailSafeHttpClient failSafeHttpClient, String baseUrl) {
        this.failSafeHttpClient = InvariantUtil.checkNotNullOrThrow(
                failSafeHttpClient, "failSafeHttpClient");
        this.baseUrl = InvariantUtil.checkNotNullNotEmptyOrThrow(
                baseUrl, "baseUrl");
    }

    /**
     * @param agencyId agency ID
     * @param bibliographicRecordId bibliographic record ID
     * @return true if records exists, otherwise false
     * @throws RecordServiceConnectorException on failure to read result entities from response
     * @throws RecordServiceConnectorUnexpectedStatusCodeException on unexpected response status code
     */
    public boolean recordExists(String agencyId, String bibliographicRecordId)
            throws RecordServiceConnectorException {
        return recordExists(agencyId, bibliographicRecordId, null);
    }

    /**
     * @param agencyId agency ID
     * @param bibliographicRecordId bibliographic record ID
     * @param params request query parameters
     * @return true if records exists, otherwise false
     * @throws RecordServiceConnectorException on failure to read result entities from response
     * @throws RecordServiceConnectorUnexpectedStatusCodeException on unexpected response status code
     */
    public boolean recordExists(String agencyId, String bibliographicRecordId, Params params)
            throws RecordServiceConnectorException {
        final Stopwatch stopwatch = new Stopwatch();
        try {
            InvariantUtil.checkNotNullNotEmptyOrThrow(agencyId, "agencyId");
            InvariantUtil.checkNotNullNotEmptyOrThrow(bibliographicRecordId, "bibliographicRecordId");
            final PathBuilder path = new PathBuilder(PATH_RECORD_EXISTS)
                    .bind(PATH_VARIABLE_AGENCY_ID, agencyId)
                    .bind(PATH_VARIABLE_BIBLIOGRAPHIC_RECORD_ID, bibliographicRecordId);
            final HttpGet httpGet = new HttpGet(failSafeHttpClient)
                    .withBaseUrl(baseUrl)
                    .withPathElements(path.build());
            if (params != null) {
                for (Map.Entry<String, Object> param : params.entrySet()) {
                    httpGet.withQueryParameter(param.getKey(), param.getValue());
                }
            }
            final Response response = httpGet.execute();
            assertResponseStatus(response, Response.Status.OK);
            return readResponseEntity(response, RecordExistsResponseEntity.class).value;
        } finally {
            LOGGER.info("recordExists({}, {}) took {} milliseconds",
                    agencyId, bibliographicRecordId,
                    stopwatch.getElapsedTime(TimeUnit.MILLISECONDS));
        }
    }

    private <T> T readResponseEntity(Response response, Class<T> tClass)
            throws RecordServiceConnectorException {
        final T entity = response.readEntity(tClass);
        if (entity == null) {
            throw new RecordServiceConnectorException (
                    String.format("Record service returned with null-valued %s entity",
                            tClass.getName()));
        }
        return entity;
    }

    private void assertResponseStatus(Response response, Response.Status expectedStatus)
            throws RecordServiceConnectorUnexpectedStatusCodeException {
        final Response.Status actualStatus =
                Response.Status.fromStatusCode(response.getStatus());
        if (actualStatus != expectedStatus) {
            throw new RecordServiceConnectorUnexpectedStatusCodeException(
                    String.format("file-store service returned with unexpected status code: %s",
                            actualStatus),
                    actualStatus.getStatusCode());
        }
    }

    /**
     * Record service parameters
     */
    public static class Params extends HashMap<String, Object> {
        public enum Key {
            /**
             * allow-deleted: used to specify whether a record should
             * be returned in case the record is deleted
             */
            ALLOW_DELETED("allow-deleted");

            private final String keyName;
            
            Key(String keyName) {
                this.keyName = keyName;
            }
            public String getKeyName() {
                return keyName;
            }
        }

        public Params withAllowDeleted(Boolean allowDeleted) {
            putOrRemoveOnNull(Key.ALLOW_DELETED, allowDeleted);
            return this;
        }

        public Optional<Boolean> getAllowDeleted() {
            return Optional.ofNullable((Boolean) this.get(Key.ALLOW_DELETED));
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

    /* This class is used internally to unmarshall
       response entities of recordExists requests */
    private static class RecordExistsResponseEntity {
        private boolean value;
        public void setValue(boolean value) {
            this.value = value;
        }
    }
}
