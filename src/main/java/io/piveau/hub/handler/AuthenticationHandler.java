package io.piveau.hub.handler;

import io.piveau.hub.util.ErrorCodeResponse;
import io.vertx.core.http.HttpHeaders;
import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.jwt.JWTOptions;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.JWTAuthHandler;

import static io.piveau.hub.util.Constants.API_KEY_AUTH;
import static io.piveau.hub.util.Constants.AUTHENTICATION_TYPE;
import static io.piveau.hub.util.Constants.JWT_AUTH;

public class AuthenticationHandler {

    private final String publicKey;
    private final String apiKey;
    private final String clientID;
    private final String BEARER = "Bearer";

    public AuthenticationHandler(String publicKey, String apiKey, String clientID) {
        this.publicKey = publicKey;
        this.apiKey = apiKey;
        this.clientID = clientID;
    }

    /**
     * Authenticate request via Api-Key or RTP token
     * @param context
     */

    public void handleAuthentication(RoutingContext context) {

        String authorization = context.request().getHeader(HttpHeaders.AUTHORIZATION);

        if (authorization == null) {
            ErrorCodeResponse.unauthorized(context, "Missing header field \'Authorization\'");
        } else if (authorization.contains(BEARER)) {
            JWTAuthOptions authOptions = new JWTAuthOptions()
                    .addPubSecKey(new PubSecKeyOptions()
                            .setAlgorithm("RS256") // probably shouldn't be hardcoded
                            .setPublicKey(publicKey))
                    .setPermissionsClaimKey("realm_access/roles") // probably shouldn't be hardcoded
                    .setJWTOptions(new JWTOptions().addAudience(clientID));

            JWTAuth authProvider = JWTAuth.create(context.vertx(), authOptions);

            JWTAuthHandler authHandler = JWTAuthHandler.create(authProvider);

            context.data().put(AUTHENTICATION_TYPE, JWT_AUTH);

            authHandler.handle(context);
        } else {
            if (this.apiKey.isEmpty()) {
                ErrorCodeResponse.internalServerError(context, "Api-Key is not specified");
            } else if (authorization.equals(this.apiKey)) {
                context.data().put(AUTHENTICATION_TYPE, API_KEY_AUTH);
                context.next();
            } else {
                ErrorCodeResponse.forbidden(context, "Incorrect Api-Key");
            }
        }
    }
}
