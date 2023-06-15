package dk.dbc.rawrepo.record;

import jakarta.ws.rs.core.Response;

public class RecordServiceConnectorNoContentStatusCodeException extends RecordServiceConnectorUnexpectedStatusCodeException {
    public RecordServiceConnectorNoContentStatusCodeException(String message) {
        super(message, Response.Status.NO_CONTENT.getStatusCode());
    }
}
