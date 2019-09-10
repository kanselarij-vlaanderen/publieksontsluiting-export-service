# Publieksontsluiting export service

Microservice that exports news for the Publieksontsluiting application

## Installation

Add the following snippet to your `docker-compose.yml`:
```
  export:
    image: kanselarij-vlaanderen/publieksontsluiting-export-service
    links:
      - database:database
    volumes:
      - ./data/exports/publieksontsluiting:/data/exports
```

The final result of the export will be written to the volume mounted in `/data/exports`.

## Configuration

The following environment variables can be configured:

* `EXPORT_BATCH_SIZE` (default: 1000): number of triples to export in batch in the final dump
