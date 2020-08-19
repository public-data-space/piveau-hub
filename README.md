# piveau hub

This is the core module for the piveau data platform. It manages syncs the triple store and the search index and provides a rich RESTful API.

## Required Piveau Services 

In order to run the hub requires some other services

- Virtuoso Triplestore (Mandatory)
- piveau-search (Highly recommended)

## Prerequisites

- Java JDK 11
- Maven
- Docker and Docker Compose

## Setup
- Clone the repository and navigate into the directory
```
$ cp conf/config.sample.json conf/config.json
$ docker-compose up -d
```
- Wait a couple of seconds. 
- Check if the search service initialised properly by browsing to http://localhost:8081
- If an error is displayed, restart the search service `docker-compose restart piveau-search`
- Start the hub
- Windows:
```
$ redeploy.bat
```
- Linux/MacOS:
```
$ ./redeploy.sh
```
- or
```
$ mvn package exec:java
```
- Browse to http://localhost:8080

## Docker Compose

- The provided Docker Compose file includes all services for local setup and development
- You can start it either entirely or select required services
- Currently it does not include the translation service and the Keycloak

## Build and Run with Docker

build:
```bash
$ mvn clean package
$ sudo docker build -t=hub .
```

run:
```bash
$ sudo docker run -p 8080:8080 -d piveau-hub
```
## Configuration 
- A sample configuration can be found in [conf/config.sample.json](conf/config.sample.json)
- The sample configuration works well with the provided docker-compose file

| Name | Description | Type |
| ------ | ------ | ------ |
| PIVEAU_HUB_SERVICE_PORT | The port for the service | number |
| PIVEAU_HUB_API_KEY | The API key of the service | string |
| PIVEAU_HUB_AUTHORIZATION_PROCESS_DATA.publicKey | Public key of the Keycloak service | string |
| PIVEAU_HUB_AUTHORIZATION_PROCESS_DATA.clientID | Client ID of backend instance | string |
| PIVEAU_HUB_AUTHORIZATION_PROCESS_DATA.client_secret | Keycloak secret for client | string |
| PIVEAU_HUB_AUTHORIZATION_PROCESS_DATA.keycloak_uri | Keycloak Host | string |
| PIVEAU_HUB_AUTHORIZATION_PROCESS_DATA.keycloak_realm | Keycloak realm name | string |
| PIVEAU_HUB_AUTHORIZATION_PROCESS_DATA.default_owner | Default owner of all keycloak resources | string |
| PIVEAU_HUB_BASE_URI | The RDF base url | string |
| PIVEAU_TRIPLESTORE_CONFIG.address | URL of the triplestore | string |
| PIVEAU_TRIPLESTORE_CONFIG.data_endpoint | Relative CRUD endpoint of the triplestore | string |
| PIVEAU_TRIPLESTORE_CONFIG.query_endpoint | Relative query endpoint of the triplestore | string |
| PIVEAU_TRIPLESTORE_CONFIG.username | Username for the triplestore | string |
| PIVEAU_TRIPLESTORE_CONFIG.password | Password for the triplestorell | string |
| PIVEAU_HUB_VALIDATOR.enabled | Enable the use of the validator | bool |
| PIVEAU_HUB_VALIDATOR.url | The URL of the validator service | string |
| PIVEAU_HUB_SEARCH_SERVICE.enabled | Enable the use of the indexing | string |
| PIVEAU_HUB_SEARCH_SERVICE.url | Host of the piveau-search service | string |
| PIVEAU_HUB_SEARCH_SERVICE.port | Port of the piveau-search service | number |
| PIVEAU_HUB_SEARCH_SERVICE.api_key | API key of the piveau-search service | string |
| PIVEAU_HUB_LOAD_VOCABULARIES | Enable the loading of RDF vocabularies | bool |
| PIVEAU_HUB_LOAD_VOCABULARIES_FETCH | Enable the loading of RDF vocabularies from remote | bool |
| PIVEAU_TRANSLATION_SERVICE.enable | Enable the machine translation service  | bool |
| PIVEAU_TRANSLATION_SERVICE.accepted_languages  | Target languages to be translated | array |
| PIVEAU_TRANSLATION_SERVICE.translation_service_url | URL of the translation servic | string |
| PIVEAU_TRANSLATION_SERVICE.callback_url | URL of the callback for the translation service | string |
| PIVEAU_DATA_UPLOAD.url | URL of the data upload service | string |
| PIVEAU_DATA_UPLOAD.service_url | Base URL of the download URL for the data | string |
| PIVEAU_DATA_UPLOAD.api_key |  API key of the data upload service | string |
| PIVEAU_HUB_CORS_DOMAINS |Remote URLs, without protocol, that are allowed to access the hub|JSON Array of strings|
| greeting | Meaningless string | string |
