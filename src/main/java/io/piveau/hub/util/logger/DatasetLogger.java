package io.piveau.hub.util.logger;

import java.util.Formatter;

public class DatasetLogger extends PiveauLogger {
    private String datasetID;
    DatasetLogger(String datasetid, Class clazz) {
        super(clazz);
        this.datasetID=datasetid;

        formatString = datasetid != null&& !datasetid.isEmpty()?"[%s (piveau-hub)] [%s (-)] [%s] ":"[%s (piveau-hub)] [- (-)] [%s] ";

    }


    @Override
    protected String getPrefixMessage() {
        StringBuilder sb = new StringBuilder();
        Formatter formatter = new Formatter(sb);
        formatter.format(formatString, component, datasetID, baseUri);
        return sb.toString();
    }
}
