package dk.dbc.rawrepo;

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
