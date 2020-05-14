# Valvas export service

Microservice that exports news for the Valvas application

## Installation

Add the following snippet to your `docker-compose.yml`:
```
  export:
    image: kanselarij-vlaanderen/valvas-export-service
    links:
      - database:database
    volumes:
      - ./data/exports/valvas:/data/exports
```

The final result of the export will be written to the volume mounted in `/data/exports`.


## Use

In order to export a zitting you must send a POST request to `/export/:uuid` being `:uuid` the uuid of the zitting we want to export.
This export can be customized with some parameters in a json object sent alongside the request.
* scope: Customizes sections of the zitting you want to export, the supported values are "news-items", "announcements" and "documents". The session info will always be exported and "documents" only can be exported if both "news-items" and "announcements" are exported
* documentNotification: including this parameter allows you to create a new ttl file for a document notification, its an object containing 2 properties the sessionDate and the documentPublicationDateTime.

Example request body:
```
{
  "scope": [ "news-items", "announcements" ],
  "documentNotification": {
    "sessionDate": "8 mei 2020",
    "documentPublicationDateTime": "11 mei 2020 om 14:00"
  }
}
```

## Configuration

The following environment variables can be configured:
* `MU_SPARQL_ENDPOINT` (default: http://database:8890/sparql): SPARQL endpoint of the internal triple store to write intermediate results to
* `VIRTUOSO_SPARQL_ENDPOINT` (default: http://virtuoso:8890/sparql): SPARQL endpoint of the Virtuoso triple store, in order to extract the ttl files.
* `KALEIDOS_SPARQL_ENDPOINT` (default: http://kaleidos:8890/sparql): SPARQL endpoint of the Kaleidos triple store
* `EXPORT_BATCH_SIZE` (default: 1000): number of triples to export in batch in the final dump

### Querying a remote triple store
To run the export querying a Kaleidos triple store on a remote server, setup an SSH tunnel with port forwarding on your `docker0` network interface (probably IP 172.17.0.1):
```
ssh kaleidos-server -L 172.17.0.1:8890:<kaleidos-triple-store-container-ip>:8890
```

Add an extra host `kaleidos` to the export service pointing to the `docker0` network.
```
  export:
    ...
    extra_hosts:
      - "kaleidos:172.17.0.1"
```
