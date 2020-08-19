package io.piveau.hub.util.logger;

import io.piveau.hub.dataobjects.DatasetHelper;

public class PiveauLoggerFactory {


    public static PiveauLogger getLogger(DatasetHelper helper, Class clazz) {
        if (helper == null) return new SimpleLogger(clazz);
        return new PiveauLogger(helper, clazz);
    }

    public static PiveauLogger getLogger(String datasetId, String catalogueId, Class clazz) {
        if (datasetId != null && !datasetId.isEmpty() && catalogueId != null && !catalogueId.isEmpty())
            return new PiveauLogger(datasetId, catalogueId, clazz);
        else if ((datasetId == null || datasetId.isEmpty()) && (catalogueId == null || catalogueId.isEmpty()))
            return new SimpleLogger(clazz);
        else if (datasetId == null || datasetId.isEmpty()) return new CatalogueLogger(catalogueId, clazz);
        else return new DatasetLogger(datasetId, clazz);
    }

    public static PiveauLogger getLogger(Class clazz) {
        return new SimpleLogger(clazz);
    }

    public static PiveauLogger getDatasetLogger(String datasetId, Class clazz) {
        return new DatasetLogger(datasetId, clazz);
    }

    public static PiveauLogger getCatalogueLogger(String catalogueId, Class clazz) {
        if (catalogueId == null || catalogueId.isEmpty()) return new SimpleLogger(clazz);
        return new CatalogueLogger(catalogueId, clazz);
    }

}
