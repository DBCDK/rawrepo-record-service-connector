/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GPLv3
 * See license text in LICENSE.txt or at https://opensource.dbc.dk/licenses/gpl-3.0/
 */

package dk.dbc.rawrepo;

import javax.ws.rs.core.Response;

public class RecordServiceConnectorNoContentStatusCodeException extends RecordServiceConnectorUnexpectedStatusCodeException {
    public RecordServiceConnectorNoContentStatusCodeException(String message) {
        super(message, Response.Status.NO_CONTENT.getStatusCode());
    }
}
