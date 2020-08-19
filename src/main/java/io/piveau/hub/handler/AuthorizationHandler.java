package io.piveau.hub.handler;

import io.piveau.hub.services.catalogues.CataloguesService;
import io.piveau.hub.util.ErrorCodeResponse;
import io.piveau.hub.util.RTPTokenHelper;
import io.piveau.hub.util.logger.PiveauLogger;
import io.piveau.hub.util.logger.PiveauLoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.RoutingContext;

import java.util.HashMap;
import java.util.Set;

import static io.piveau.hub.util.Constants.API_KEY_AUTH;
import static io.piveau.hub.util.Constants.AUTHENTICATION_TYPE;
import static io.piveau.hub.util.Constants.JWT_AUTH;


/**
 * TODO check performance level for PUT dataset
 */

public class AuthorizationHandler {

    private PiveauLogger log = PiveauLoggerFactory.getLogger(getClass());

    final private String UPDATE = "update";
    final private String CREATE = "create";
    final private String DELETE_RESOURCE = "delete";

    final private String PUT = "PUT";
    final private String POST = "POST";
    final private String DELETE_REQUEST = "DELETE";

    private JsonObject authorizationProcessData;
    private String base_uri;

    public AuthorizationHandler(JsonObject authorizationProcessData, String base_uri) {
        this.authorizationProcessData = authorizationProcessData;
        this.base_uri = base_uri;
    }

    /**
     * Authorize all requests that must be authenticated according to /src/main/resources/webroot/openapi.yaml and /doc/requests.http
     * Meaning most PUT requests (at the moment only PUT dataset and PUT catalogue)
     * An RTP token is needed for such authorization
     * @param context
     */

    public void handleAuthorization(RoutingContext context) {
        if (context.data().get(AUTHENTICATION_TYPE).equals(JWT_AUTH)) {

            final User user = context.user();

            if (user == null) {
                ErrorCodeResponse.badRequest(context, "No user");
            } else {

                String requestMethod = context.request().method().toString();

                /**** To better use the RTP token in java, create a HashMap with all the user's permissions for each resource ****/

                RTPTokenHelper rtpTokenHelper = new RTPTokenHelper(context);
                final HashMap<String, String> userPermissionsAndResources = rtpTokenHelper.initiate();
                switch (requestMethod) {
                    case POST:
                    case PUT:
                        /**** if request is a put or Post dataset (create dataset) check if user has permission to update the catalogue in which the dataset resides ****/
                        if (context.request().query() != null) {

                            String catalogueID = context.request().getParam("catalogue");

                            if (catalogueID != null) {

                                /**** get all resources user has any permission for ****/
                                Set<String> userResources = userPermissionsAndResources.keySet();

                                /**** check if user has update permission for the requested catalogue ****/
                                boolean resource = false;
                                for (String key : userResources) {
                                    if (key.contains(catalogueID) == true) {
                                        resource = true;
                                        if (userPermissionsAndResources.get(key).contains(UPDATE)) {
                                            log.debug("Catalogue is being updated");
                                            context.next();
                                        } else {
                                            ErrorCodeResponse.forbidden(context, "No permission for this resource");
                                        }
                                        break;
                                    }
                                }

                                if (resource == false) {
                                    ErrorCodeResponse.forbidden(context, "No permission for this resource");
                                }
                            } else {
                                ErrorCodeResponse.badRequest(context, "Bad query");
                            }
                        /**** if request is PUT catalogue (create catalogue) ****/
                        } else {
                            String catalogueID = context.request().getParam("id");
                            if (catalogueID != null) {
                                /**** check if catalogue already exists. if yes, update it. if not, create it if permission update on resource /* exists ****/

                                CataloguesService cataloguesServiceHelper = CataloguesService.createProxy(context.vertx(), CataloguesService.SERVICE_ADDRESS);
                                cataloguesServiceHelper.existenceCheckCatalogue(catalogueID, ar -> {
                                    /**** catalogue already exists -> check if user has update permission ****/
                                    if (ar.succeeded()) {
                                        Set<String> userResources = userPermissionsAndResources.keySet();
                                        boolean resource = false;

                                        for (String key : userResources) {
                                            if (key.contains(catalogueID)) {
                                                resource = true;
                                                if (userPermissionsAndResources.get(key).contains(UPDATE)) {
                                                    log.debug("Catalogue is being updated");
                                                    context.next();
                                                } else {
                                                    ErrorCodeResponse.forbidden(context, "No permission for this resource");
                                                }
                                                break;
                                            }
                                        }

                                        if (resource == false) {
                                            ErrorCodeResponse.forbidden(context, "No permission for this resource");
                                        }
                                    /**** catalogue doesn't exist -> check if user has create permission ****/
                                    } else {
                                        Set<String> userResources = userPermissionsAndResources.keySet();
                                        boolean resource = false;

                                        for (String key : userResources) {
                                            if (key.contains("*") == true) {
                                                resource = true;
                                                if (userPermissionsAndResources.get(key).contains(CREATE)) {
                                                    /**** necessary variables to later create keycloak resource in putCatalogueHandler ****/
                                                    /**
                                                     * TODO create keycloak resource here?
                                                     */
                                                    context.data().put("catalogue", catalogueID);
                                                    context.data().put("clientID", authorizationProcessData.getString("clientID"));
                                                    context.data().put("client_secret", authorizationProcessData.getString("client_secret"));
                                                    context.data().put("keycloak_uri", authorizationProcessData.getString("keycloak_uri"));
                                                    context.data().put("keycloak_realm", authorizationProcessData.getString("keycloak_realm"));
                                                    context.data().put("base_uri", base_uri);
                                                    context.data().put("owner", context.user().principal().getString("preferred_username"));
                                                    log.info("PUT catalogue is creating");
                                                    context.next();
                                                } else {
                                                    ErrorCodeResponse.forbidden(context, "No permission for this resource");
                                                }
                                                break;
                                            }
                                        }
                                        if (resource == false) {
                                            ErrorCodeResponse.forbidden(context, "No permission for this resource");
                                        }
                                    }
                                });
                            } else {
                                ErrorCodeResponse.badRequest(context);
                            }
                        }
                        break;

                    /**** if request is delete a dataset ****/
                    case DELETE_REQUEST:
                        /**** if a dataset is to be deleted -> check permission for catalogue in which dataset is to be deleted. Update permission is needed to update the catalogue. ****/
                        if (context.request().query() != null) { // context.request().query().contains("datasets").contains("catalogue") --> would make this valid only for PUT dataset

                            String catalogueID = context.request().getParam("catalogue");

                            if (catalogueID != null) {

                                Set<String> userResources = userPermissionsAndResources.keySet();
                                boolean resource = false;

                                for (String key : userResources) {
                                    if (key.contains(catalogueID) == true) {
                                        resource = true;
                                        if (userPermissionsAndResources.get(key).contains("update")) {
                                            log.debug("DELETE dataset");
                                            context.next();
                                        } else {
                                            ErrorCodeResponse.forbidden(context, "No permission for this resource");
                                        }
                                        break;
                                    }
                                }

                                if (resource == false) {
                                    ErrorCodeResponse.forbidden(context, "No permission for this resource");
                                }
                            } else {
                                ErrorCodeResponse.badRequest(context);
                            }

                        /**** if request is delete a catalgoue ****/
                        } else {
                            if (!context.request().path().isEmpty()) {

                                String catalogueID = context.request().getParam("id");

                                if (catalogueID != null) {
                                    Set<String> userResources = userPermissionsAndResources.keySet();

                                    boolean resource = false;

                                    for (String key : userResources) {
                                        if (key.contains(catalogueID) == true) {
                                            resource = true;
                                            if (userPermissionsAndResources.get(key).contains(DELETE_RESOURCE)) {
                                                log.debug("DELETE catalogue");
                                                /**** variables needed to delete catalogue in keycloak ****/
                                                context.data().put("catalogue", context.request().getParam("id"));
                                                context.data().put("clientID", authorizationProcessData.getString("clientID"));
                                                context.data().put("client_secret", authorizationProcessData.getString("client_secret"));
                                                context.data().put("keycloak_uri", authorizationProcessData.getString("keycloak_uri"));
                                                context.data().put("keycloak_realm", authorizationProcessData.getString("keycloak_realm"));
                                                log.debug("DELETE catalogue went through");
                                                context.next();
                                            } else {
                                                ErrorCodeResponse.forbidden(context, "No permission for this resource");
                                            }
                                            break;
                                        }
                                    }
                                    if (resource == false) {
                                        ErrorCodeResponse.forbidden(context, "No permission for this resource");
                                    }
                                } else {
                                    ErrorCodeResponse.badRequest(context, "Bad query");
                                }
                            } else {
                                ErrorCodeResponse.badRequest(context);
                            }
                        }
                        break;
                    default:
                        ErrorCodeResponse.unauthorized(context);
                }
            }

        /**** if authentication was done with an API key, no authorization process is necessary, so move along ****/
        } else if (context.data().get(AUTHENTICATION_TYPE).equals(API_KEY_AUTH)) {
            /**** if PUT dataset -> just go through; if PUT catalogue -> set necessary variables in context.data() fields ****/

            if (context.request().method().toString().equals(PUT) || context.request().method().toString().equals(POST)) {
                /**** PUT catalogue ****/
                if (context.request().query() == null) {
                    /**
                     * TODO transfer all variables into a variable class instead of putting them in the context ?
                     */
                    context.data().put("owner", authorizationProcessData.getString("default_owner"));
                    context.data().put("catalogue", context.request().getParam("id"));
                    context.data().put("clientID", authorizationProcessData.getString("clientID"));
                    context.data().put("client_secret", authorizationProcessData.getString("client_secret"));
                    context.data().put("keycloak_uri", authorizationProcessData.getString("keycloak_uri"));
                    context.data().put("keycloak_realm", authorizationProcessData.getString("keycloak_realm"));
                    context.data().put("base_uri", base_uri);
                    log.debug("PUT catalogue with API key went through");
                    context.next();
                } else {
                    /**** PUT dataset ****/
                    log.debug("PUT dataset with API key went through");
                    context.next();
                }
            } else if (context.request().method().toString().equals(DELETE_REQUEST)) {
                /**** delete catalogue ****/
                if (context.request().query() == null) {
                    log.debug("DELETE catalogue with API key went through");
                    context.data().put("catalogue", context.request().getParam("id"));
                    context.data().put("clientID", authorizationProcessData.getString("clientID"));
                    context.data().put("client_secret", authorizationProcessData.getString("client_secret"));
                    context.data().put("keycloak_uri", authorizationProcessData.getString("keycloak_uri"));
                    context.data().put("keycloak_realm", authorizationProcessData.getString("keycloak_realm"));
                    context.next();
                } else {
                    /**** delete dataset ****/
                    log.info("DELETE dataset with API key went through");
                    context.next();
                }
            }
        } else {
            ErrorCodeResponse.unauthorized(context, "No authentication");
        }
    }
}
