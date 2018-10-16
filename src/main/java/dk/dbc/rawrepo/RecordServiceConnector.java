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
    private static final String PATH_RECORD_CONTENT = String.format("/api/v1/record/{%s}/{%s}/content",
            PATH_VARIABLE_AGENCY_ID, PATH_VARIABLE_BIBLIOGRAPHIC_RECORD_ID);
    private static final String PATH_RECORD_META = String.format("/api/v1/record/{%s}/{%s}/meta",
            PATH_VARIABLE_AGENCY_ID, PATH_VARIABLE_BIBLIOGRAPHIC_RECORD_ID);
    private static final String PATH_RECORD_DATA = String.format("/api/v1/record/{%s}/{%s}",
            PATH_VARIABLE_AGENCY_ID, PATH_VARIABLE_BIBLIOGRAPHIC_RECORD_ID);
    private static final String PATH_RECORD_DATA_COLLECTION = String.format("/api/v1/records/{%s}/{%s}",
            PATH_VARIABLE_AGENCY_ID, PATH_VARIABLE_BIBLIOGRAPHIC_RECORD_ID);
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

    public void close() {
        failSafeHttpClient.getClient().close();
    }

    /**
     * @param agencyId agency ID
     * @param bibliographicRecordId bibliographic record ID
     * @return true if records exists, otherwise false
     * @throws RecordServiceConnectorException on failure to read result entity from response
     * @throws RecordServiceConnectorUnexpectedStatusCodeException on unexpected response status code
     */
    public boolean recordExists(int agencyId, String bibliographicRecordId)
            throws RecordServiceConnectorException {
        return recordExists(Integer.toString(agencyId), bibliographicRecordId, null);
    }

    /**
     * @param agencyId agency ID
     * @param bibliographicRecordId bibliographic record ID
     * @return true if records exists, otherwise false
     * @throws RecordServiceConnectorException on failure to read result entity from response
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
     * @throws RecordServiceConnectorException on failure to read result entity from response
     * @throws RecordServiceConnectorUnexpectedStatusCodeException on unexpected response status code
     */
    public boolean recordExists(int agencyId, String bibliographicRecordId, Params params)
            throws RecordServiceConnectorException {
        return recordExists(Integer.toString(agencyId), bibliographicRecordId, params);
    }

    /**
     * @param agencyId agency ID
     * @param bibliographicRecordId bibliographic record ID
     * @param params request query parameters
     * @return true if records exists, otherwise false
     * @throws RecordServiceConnectorException on failure to read result entity from response
     * @throws RecordServiceConnectorUnexpectedStatusCodeException on unexpected response status code
     */
    public boolean recordExists(String agencyId, String bibliographicRecordId, Params params)
            throws RecordServiceConnectorException {
        final Stopwatch stopwatch = new Stopwatch();
        try {
            return sendRequest(PATH_RECORD_EXISTS, agencyId, bibliographicRecordId, params, RecordExistsResponseEntity.class).value;
        } finally {
            LOGGER.info("recordExists({}, {}) took {} milliseconds",
                    agencyId, bibliographicRecordId,
                    stopwatch.getElapsedTime(TimeUnit.MILLISECONDS));
        }
    }

    /**
     * @param agencyId agency ID
     * @param bibliographicRecordId bibliographic record ID
     * @return record content as MarcXchange XML
     * @throws RecordServiceConnectorException on failure to read result entity from response
     * @throws RecordServiceConnectorUnexpectedStatusCodeException on unexpected response status code
     */
    public byte[] getRecordContent(int agencyId, String bibliographicRecordId)
            throws RecordServiceConnectorException {
        return getRecordContent(Integer.toString(agencyId), bibliographicRecordId, null);
    }

    /**
     * @param agencyId agency ID
     * @param bibliographicRecordId bibliographic record ID
     * @return record content as MarcXchange XML
     * @throws RecordServiceConnectorException on failure to read result entity from response
     * @throws RecordServiceConnectorUnexpectedStatusCodeException on unexpected response status code
     */
    public byte[] getRecordContent(String agencyId, String bibliographicRecordId)
            throws RecordServiceConnectorException {
        return getRecordContent(agencyId, bibliographicRecordId, null);
    }

    /**
     * @param agencyId agency ID
     * @param bibliographicRecordId bibliographic record ID
     * @param params request query parameters
     * @return record content as MarcXchange XML
     * @throws RecordServiceConnectorException on failure to read result entity from response
     * @throws RecordServiceConnectorUnexpectedStatusCodeException on unexpected response status code
     */
    public byte[] getRecordContent(int agencyId, String bibliographicRecordId, Params params)
            throws RecordServiceConnectorException {
        return getRecordContent(Integer.toString(agencyId), bibliographicRecordId, params);
    }

    /**
     * @param agencyId agency ID
     * @param bibliographicRecordId bibliographic record ID
     * @param params request query parameters
     * @return record content as MarcXchange XML
     * @throws RecordServiceConnectorException on failure to read result entity from response
     * @throws RecordServiceConnectorUnexpectedStatusCodeException on unexpected response status code
     */
    private byte[] getRecordContent(String agencyId, String bibliographicRecordId, Params params)
            throws RecordServiceConnectorException {
        final Stopwatch stopwatch = new Stopwatch();
        try {
            return sendRequest(PATH_RECORD_CONTENT, agencyId, bibliographicRecordId, params, byte[].class);
        } finally {
            LOGGER.info("getRecordContent({}, {}) took {} milliseconds",
                    agencyId, bibliographicRecordId,
                    stopwatch.getElapsedTime(TimeUnit.MILLISECONDS));
        }
    }

    /**
     * @param agencyId agency ID
     * @param bibliographicRecordId bibliographic record ID
     * @return record content as RecordData object
     * @throws RecordServiceConnectorException on failure to read result entity from response
     * @throws RecordServiceConnectorUnexpectedStatusCodeException on unexpected response status code
     */
    public RecordData getRecordData(int agencyId, String bibliographicRecordId)
            throws RecordServiceConnectorException {
        return getRecordData(Integer.toString(agencyId), bibliographicRecordId, null);
    }

    /**
     * @param agencyId agency ID
     * @param bibliographicRecordId bibliographic record ID
     * @return record content as RecordData object
     * @throws RecordServiceConnectorException on failure to read result entity from response
     * @throws RecordServiceConnectorUnexpectedStatusCodeException on unexpected response status code
     */
    public RecordData getRecordData(String agencyId, String bibliographicRecordId)
            throws RecordServiceConnectorException {
        return getRecordData(agencyId, bibliographicRecordId, null);
    }

    /**
     * @param agencyId agency ID
     * @param bibliographicRecordId bibliographic record ID
     * @param params request query parameters
     * @return record content as RecordData object
     * @throws RecordServiceConnectorException on failure to read result entity from response
     * @throws RecordServiceConnectorUnexpectedStatusCodeException on unexpected response status code
     */
    public RecordData getRecordData(int agencyId, String bibliographicRecordId, Params params)
            throws RecordServiceConnectorException {
        return getRecordData(Integer.toString(agencyId), bibliographicRecordId, params);
    }

    /**
     * @param agencyId agency ID
     * @param bibliographicRecordId bibliographic record ID
     * @param params request query parameters
     * @return record content as RecordData object
     * @throws RecordServiceConnectorException on failure to read result entity from response
     * @throws RecordServiceConnectorUnexpectedStatusCodeException on unexpected response status code
     */
    public RecordData getRecordData(String agencyId, String bibliographicRecordId, Params params)
            throws RecordServiceConnectorException {
        final Stopwatch stopwatch = new Stopwatch();
        try {
            return sendRequest(PATH_RECORD_DATA, agencyId, bibliographicRecordId, params, RecordData.class);
        } finally {
            LOGGER.info("getRecordData({}, {}) took {} milliseconds",
                    agencyId, bibliographicRecordId,
                    stopwatch.getElapsedTime(TimeUnit.MILLISECONDS));
        }
    }

    /**
     * @param agencyId agency ID
     * @param bibliographicRecordId bibliographic record ID
     * @return record content as map of agencyId:RecordData-object
     * @throws RecordServiceConnectorException on failure to read result entity from response
     * @throws RecordServiceConnectorUnexpectedStatusCodeException on unexpected response status code
     */
    public HashMap<String, RecordData> getRecordDataCollection(int agencyId, String bibliographicRecordId)
            throws RecordServiceConnectorException {
        return getRecordDataCollection(Integer.toString(agencyId), bibliographicRecordId, null);
    }

    /**
     * @param agencyId agency ID
     * @param bibliographicRecordId bibliographic record ID
     * @return record content as map of agencyId:RecordData-object
     * @throws RecordServiceConnectorException on failure to read result entity from response
     * @throws RecordServiceConnectorUnexpectedStatusCodeException on unexpected response status code
     */
    public HashMap<String, RecordData> getRecordDataCollection(String agencyId, String bibliographicRecordId)
            throws RecordServiceConnectorException {
        return getRecordDataCollection(agencyId, bibliographicRecordId, null);
    }

    /**
     * @param agencyId agency ID
     * @param bibliographicRecordId bibliographic record ID
     * @param params request query parameters
     * @return record content as map of agencyId:RecordData-object
     * @throws RecordServiceConnectorException on failure to read result entity from response
     * @throws RecordServiceConnectorUnexpectedStatusCodeException on unexpected response status code
     */
    public HashMap<String, RecordData> getRecordDataCollection(int agencyId, String bibliographicRecordId, Params params)
            throws RecordServiceConnectorException {
        return getRecordDataCollection(Integer.toString(agencyId), bibliographicRecordId, params);
    }

    /**
     * @param agencyId agency ID
     * @param bibliographicRecordId bibliographic record ID
     * @return record content as map of agencyId:RecordData-object
     * @param params request query parameters
     * @throws RecordServiceConnectorException on failure to read result entity from response
     * @throws RecordServiceConnectorUnexpectedStatusCodeException on unexpected response status code
     */
    public HashMap<String, RecordData> getRecordDataCollection(String agencyId, String bibliographicRecordId, Params params)
            throws RecordServiceConnectorException {
        final Stopwatch stopwatch = new Stopwatch();
        try {
            return sendRequest(PATH_RECORD_DATA_COLLECTION, agencyId, bibliographicRecordId, params, RecordDataCollection.class).toMap();
        } finally {
            LOGGER.info("getRecordDataCollection({}, {}) took {} milliseconds",
                    agencyId, bibliographicRecordId,
                    stopwatch.getElapsedTime(TimeUnit.MILLISECONDS));
        }
    }

    /**
     * @param agencyId agency ID
     * @param bibliographicRecordId bibliographic record ID
     * @return record content as RecordData object
     * @throws RecordServiceConnectorException on failure to read result entity from response
     * @throws RecordServiceConnectorUnexpectedStatusCodeException on unexpected response status code
     */
    public RecordData getRecordMeta(String agencyId, String bibliographicRecordId)
            throws RecordServiceConnectorException {
        return getRecordMeta(agencyId, bibliographicRecordId, null);
    }

    /**
     * @param agencyId agency ID
     * @param bibliographicRecordId bibliographic record ID
     * @param params request query parameters
     * @return record content as RecordData object
     * @throws RecordServiceConnectorException on failure to read result entity from response
     * @throws RecordServiceConnectorUnexpectedStatusCodeException on unexpected response status code
     */
    public RecordData getRecordMeta(String agencyId, String bibliographicRecordId, Params params)
            throws RecordServiceConnectorException {
        final Stopwatch stopwatch = new Stopwatch();
        try {
            return sendRequest(PATH_RECORD_META, agencyId, bibliographicRecordId, params, RecordData.class);
        } finally {
            LOGGER.info("getRecordMeta({}, {}) took {} milliseconds",
                    agencyId, bibliographicRecordId,
                    stopwatch.getElapsedTime(TimeUnit.MILLISECONDS));
        }
    }

    private <T> T sendRequest(String basePath, String agencyId, String bibliographicRecordId, Params params, Class<T> tClass)
            throws RecordServiceConnectorException {
        InvariantUtil.checkNotNullNotEmptyOrThrow(agencyId, "agencyId");
        InvariantUtil.checkNotNullNotEmptyOrThrow(bibliographicRecordId, "bibliographicRecordId");
        final PathBuilder path = new PathBuilder(basePath)
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
        return readResponseEntity(response, tClass);
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
                    String.format("Record service returned with unexpected status code: %s",
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
            ALLOW_DELETED("allow-deleted"),
            /**
             * exclude-dbc-fields: used to specify if DBC letter fields
             * should be returned
             */
            EXCLUDE_DBC_FIELDS("exclude-dbc-fields"),
            /**
             * keep-aut-fields: if true, subfields *5 and *6 will not
             * be removed when expanded
             */
            KEEP_AUT_FIELDS("keep-aut-fields"),
            /**
             * mode: used to determine whether the returned record
             * content should be raw, merged or expanded
             */
            MODE("mode"),
            /**
             * use-parent-agency: setting use-parent-agency to true
             * returns record with the parent agency
             */
            USE_PARENT_AGENCY("use-parent-agency");

            private final String keyName;
            
            Key(String keyName) {
                this.keyName = keyName;
            }
            public String getKeyName() {
                return keyName;
            }
        }

        public enum Mode {
            RAW, MERGED, EXPANDED;

            @Override
            public String toString() {
                return name().toLowerCase();
            }
        }

        public Params withAllowDeleted(Boolean allowDeleted) {
            putOrRemoveOnNull(Key.ALLOW_DELETED, allowDeleted);
            return this;
        }

        public Optional<Boolean> getAllowDeleted() {
            return Optional.ofNullable((Boolean) this.get(Key.ALLOW_DELETED));
        }

        public Params withExcludeDbcFields(Boolean excludeDbcFields) {
            putOrRemoveOnNull(Key.EXCLUDE_DBC_FIELDS, excludeDbcFields);
            return this;
        }

        public Optional<Boolean> getExcludeDbcFields() {
            return Optional.ofNullable((Boolean) this.get(Key.EXCLUDE_DBC_FIELDS));
        }

        public Params withKeepAutFields(Boolean keepAutFields) {
            putOrRemoveOnNull(Key.KEEP_AUT_FIELDS, keepAutFields);
            return this;
        }

        public Optional<Boolean> getKeepAutFields() {
            return Optional.ofNullable((Boolean) this.get(Key.KEEP_AUT_FIELDS));
        }

        public Params withMode(Mode mode) {
            putOrRemoveOnNull(Key.MODE, mode);
            return this;
        }

        public Optional<Mode> getMode() {
            return Optional.ofNullable((Mode) this.get(Key.MODE));
        }

        public Params withUseParentAgency(Boolean useParentAgency) {
            putOrRemoveOnNull(Key.USE_PARENT_AGENCY, useParentAgency);
            return this;
        }

        public Optional<Boolean> getUseParentAgency() {
            return Optional.ofNullable((Boolean) this.get(Key.USE_PARENT_AGENCY));
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
