/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GPLv3
 * See license text in LICENSE.txt or at https://opensource.dbc.dk/licenses/gpl-3.0/
 */

package dk.dbc.rawrepo.dump;

import dk.dbc.rawrepo.dto.ParamsValidation;

public class RecordDumpServiceConnectorUnexpectedStatusCodeValidationException extends RecordDumpServiceConnectorUnexpectedStatusCodeException {

    private ParamsValidation paramsValidation;

    public RecordDumpServiceConnectorUnexpectedStatusCodeValidationException(ParamsValidation paramsValidation, int statusCode) {
        super("Validation exception", statusCode);
        this.paramsValidation = paramsValidation;
    }

    public ParamsValidation getParamsValidation() {
        return paramsValidation;
    }
}
