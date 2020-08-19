package io.piveau.hub.util;

import io.piveau.hub.util.logger.PiveauLogger;
import io.piveau.hub.util.logger.PiveauLoggerFactory;
import io.vertx.core.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.WebClient;

import java.util.HashMap;


public class RTPTokenHelper {

    private User user;
    private RoutingContext context;
    private PiveauLogger log = PiveauLoggerFactory.getLogger(getClass());


    public RTPTokenHelper(RoutingContext context) {
        this.user = context.user();
        this.context = context;
    }

    public HashMap<String, String> initiate() {

        HashMap<String, String> userPermissionsAndResources = new HashMap<String, String>();

        if (this.user.principal().getJsonObject("authorization") != null) {

            JsonObject userAuthorization = user.principal().getJsonObject("authorization");
            if (userAuthorization.getJsonArray("permissions").size() > 0) {

                for (int i = 0; i < userAuthorization.getJsonArray("permissions").size(); ++i) {
                    String userResource = "";

                    if (userAuthorization.getJsonArray("permissions").getJsonObject(i).getString("rsname") != null) {
                        userResource = userAuthorization.getJsonArray("permissions").getJsonObject(i).getString("rsname");
                    } else {
                        ErrorCodeResponse.forbidden(context, "No permissions");
                    }

                    String userPermission = "";

                    if (userAuthorization.getJsonArray("permissions").getJsonObject(i).getJsonArray("scopes") != null) {
                        JsonArray userScope = userAuthorization.getJsonArray("permissions").getJsonObject(i).getJsonArray("scopes");
                        userPermission = userScope.getString(0);
                        if (userScope.size() > 1) {
                            for (int j = 1; j < userScope.size(); ++j) {
                                userPermission = userPermission.concat(", ").concat(userScope.getString(j));
                            }
                        }
                    } else {
                        ErrorCodeResponse.badRequest(context, "Invalid token");
                    }

                    userPermissionsAndResources.put(userResource, userPermission);
                }

                return userPermissionsAndResources;

            } else {
                ErrorCodeResponse.forbidden(context, "No permission");
            }
        } else {
            ErrorCodeResponse.badRequest(context, "Invalid token");
        }

        return null;
    }

    public MultiMap createMultiMap() {
        return MultiMap.caseInsensitiveMultiMap().set("grant_type", "client_credentials")
                .set("client_id", context.data().get("clientID").toString())
                .set("client_secret", context.data().get("client_secret").toString());
    }

    public JsonObject createResource(String owner) {
        return new JsonObject().put("name", context.data().get("base_uri").toString() + "id/catalogue/" + context.data().get("catalogue"))
                .put("owner", owner)
                .put("ownerManagedAccess", true)
                .put("displayName", context.data().get("base_uri").toString() + "id/catalogue/" + context.data().get("catalogue"))
                .put("attributes", new JsonArray())
                .put("type", "urn:" + context.data().get("clientID").toString() + ":resources:catalog")
                .put("uris", new JsonArray().add(context.data().get("base_uri").toString() + "id/catalogue/" + context.data().get("catalogue")))
                .put("resource_scopes", new JsonArray().add(new JsonObject().put("name", "update")).add(new JsonObject().put("name", "delete")))
                .put("scopes", new JsonArray().add(new JsonObject().put("name", "update")).add(new JsonObject().put("name", "delete")));
    }

    public void createKeycloakResource(WebClient client, String owner, String catalogueID) {

        if (catalogueID !=  null) {
            try {
                client.postAbs(context.data().get("keycloak_uri").toString() + "/auth/realms/" + context.data().get("keycloak_realm").toString() + "/protocol/openid-connect/token")
                        .sendForm(createMultiMap(), ar -> {
                            if (ar.succeeded()) {
                                JsonObject response = ar.result().bodyAsJsonObject();
                                String access_token = response.getString("access_token");

                                if (!access_token.equals(null)) {
                                    try {
                                        client.postAbs(context.data().get("keycloak_uri").toString() + "/auth/realms/" + context.data().get("keycloak_realm").toString() + "/authz/protection/resource_set/")
                                                .putHeader("Content-Type", "application/json")
                                                .bearerTokenAuthentication(access_token)
                                                .timeout(1000)
                                                .sendJson(createResource(owner), ar2 -> {
                                                    if (ar.succeeded()) {
                                                        log.info("Created keycloak resource");
                                                    } else {
                                                        log.error("Failed to create keycloak resource for catalgoue " + catalogueID);

                                                    }
                                                });
                                    } catch (Exception e) {
                                        log.error(e.getMessage());
                                    }
                                } else {
                                    log.error(response.encodePrettily());
                                }

                            } else {
                                log.error(ar.cause().getMessage());
                            }
                        });
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        }
    }

    public void deleteKeycloakResource(WebClient client) {
        if (context.data().get("catalogue") !=  null) {
            try {
                client.postAbs(context.data().get("keycloak_uri").toString() + "/auth/realms/" + context.data().get("keycloak_realm").toString() + "/protocol/openid-connect/token")
                        .timeout(1000)
                        .sendForm(createMultiMap(), ar -> {
                            if (ar.succeeded()) {

                                JsonObject response = ar.result().bodyAsJsonObject();
                                String access_token = response.getString("access_token");

                                if (!access_token.equals(null)) {

                                    try {
                                        client.getAbs(context.data().get("keycloak_uri").toString() + "/auth/realms/" + context.data().get("keycloak_realm").toString() + "/authz/protection/resource_set")
                                                .addQueryParam("name", context.data().get("catalogue").toString())
                                                .bearerTokenAuthentication(access_token)
                                                .timeout(1000)
                                                .send(ar_1 -> {
                                                    if (ar_1.succeeded()) {
                                                        String rsid = ar_1.result().bodyAsString().replace("[\"", "").replace("\"]", "");
                                                        if (rsid.contains(",")) {
                                                            log.debug("Keycloak gave a resource more than one ID");
                                                            String[] ids = rsid.split(",");

                                                            for (String id : ids) {
                                                                try {
                                                                    client.deleteAbs(context.data().get("keycloak_uri").toString() + "/auth/realms/" + context.data().get("keycloak_realm").toString() + "/authz/protection/resource_set/" + id)
                                                                            .bearerTokenAuthentication(access_token)
                                                                            .timeout(1000)
                                                                            .send(ar2 -> {
                                                                                if (ar.succeeded()) {
                                                                                    log.info("Resource " + context.data().get("catalogue") + " deleted successfully");
                                                                                } else {
                                                                                    log.error("Failed to delete resource " + context.data().get("catalogue"));

                                                                                }
                                                                            });
                                                                } catch (Exception e) {
                                                                    log.error(e.getMessage());
                                                                }
                                                            }
                                                        }
                                                        try {
                                                            client.deleteAbs(context.data().get("keycloak_uri").toString() + "/auth/realms/" + context.data().get("keycloak_realm").toString() + "/authz/protection/resource_set/" + rsid)
                                                                    .bearerTokenAuthentication(access_token)
                                                                    .timeout(1000)
                                                                    .send(ar2 -> {
                                                                        if (ar.succeeded()) {
                                                                            log.info("Resource " + context.data().get("catalogue") + " deleted successfully");
                                                                        } else {
                                                                            log.error("Failed to delete resource " + context.data().get("catalogue"));

                                                                        }
                                                                    });
                                                        } catch (Exception e) {
                                                            log.error(e.getMessage());
                                                        }
                                                    } else {
                                                        log.error("Failed to delete resource " + context.data().get("catalogue"));
                                                    }
                                                });
                                    } catch (Exception e) {
                                        log.error(e.getMessage());
                                    }
                                } else {
                                    log.error(response.encodePrettily());
                                }
                            } else {
                                log.error(ar.cause().getMessage());
                            }
                        });
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        } else {
            log.error("Could not create keycloak resource - catalogueID missing");
        }
    }
}
