package io.piveau.hub.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

final public class Constants {

    static public final String ENV_PIVEAU_TRIPLESTORE_CONFIG = "PIVEAU_TRIPLESTORE_CONFIG";
    static public final String ENV_PIVEAU_HUB_SERVICE_PORT = "PIVEAU_HUB_SERVICE_PORT";
    static public final String ENV_PIVEAU_HUB_API_KEY = "PIVEAU_HUB_API_KEY";
    static public final String ENV_PIVEAU_HUB_AUTHORIZATION_PROCESS_DATA = "PIVEAU_HUB_AUTHORIZATION_PROCESS_DATA";
    static public final String ENV_PIVEAU_HUB_BASE_URI = "PIVEAU_HUB_BASE_URI";
    static public final String ENV_PIVEAU_HUB_VALIDATOR = "PIVEAU_HUB_VALIDATOR";
    static public final String ENV_PIVEAU_HUB_SEARCH_SERVICE = "PIVEAU_HUB_SEARCH_SERVICE";
    static public final String ENV_PIVEAU_TRANSLATION_SERVICE = "PIVEAU_TRANSLATION_SERVICE";
    static public final String ENV_PIVEAU_DATA_UPLOAD = "PIVEAU_DATA_UPLOAD";

    static public final String ENV_PIVEAU_HUB_LOAD_VOCABULARIES = "PIVEAU_HUB_LOAD_VOCABULARIES";
    static public final String ENV_PIVEAU_HUB_LOAD_VOCABULARIES_FETCH = "PIVEAU_HUB_LOAD_VOCABULARIES_FETCH";

    static public final String ENV_PIVEAU_HUB_ELASTICSEARCH_ADDRESS = "PIVEAU_HUB_ELASTICSEARCH_ADDRESS";

    static public final String ENV_PIVEAU_HUB_CORS_DOMAINS ="PIVEAU_HUB_CORS_DOMAINS";

    static public final String ENV_PIVEAU_CLUSTER_CONFIG ="PIVEAU_CLUSTER_CONFIG";

    public static final List<String> ALLOWED_CONTENT_TYPES = Collections.unmodifiableList(Arrays.asList(
        "application/rdf+xml",
        "application/ld+json",
        "text/turtle",
        "text/n3",
        "application/trig",
        "application/n-triples"
    ));

    // Authentication via JWT
    static public final String AUTHENTICATION_TYPE = "auth";
    static public final String JWT_AUTH = "jwtAuth";
    static public final String API_KEY_AUTH = "apiKey";

    //static public final String PUT_ROLE = "provider";
    //static public final String DELETE_ROLE = "developer";

}
