package io.piveau.hub.util.logger;

import java.util.Formatter;



class CatalogueLogger extends PiveauLogger {

    private String catalogueId;


    CatalogueLogger(String catalogueId, Class clazz) {
        super(clazz);
        formatString = catalogueId != null&& !catalogueId.isEmpty()?"[%s (piveau-hub)] [- (%s)] [%s] ":"[%s (piveau-hub)] [- (-)] [%s] ";
        this.catalogueId = catalogueId;
    }

    @Override
    protected String getPrefixMessage() {
        StringBuilder sb = new StringBuilder();
        Formatter formatter = new Formatter(sb);
        formatter.format(formatString, component, catalogueId, baseUri);
        return sb.toString();
    }



}
