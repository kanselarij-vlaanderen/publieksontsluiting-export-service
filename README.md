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

## Configuration

The following environment variables can be configured:
* `MU_SPARQL_ENDPOINT` (default: http://database:8890/sparql): SPARQL endpoint of the internal triple store to write intermediate results to
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
