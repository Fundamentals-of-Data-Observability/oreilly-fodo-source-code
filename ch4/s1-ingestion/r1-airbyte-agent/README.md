# Data Observability Agent For Airbyte

## Goals

Inject data observability capabilities in Airbyte, by intercepting using ASM the process of replication of the data.

This agent is based on ASM that will update the bytecode of Airbyte (workers) with additional DO functionalities.

There are therefore 2 projects:
- `airbyte-instrumentation`: the `javaagent` that will update the bytecode calling code provided on the CP by the below project;
- `airbyte-agent`: the library generating and publishing data observations.

The agent intercepts mainly `DefaultReplicationWorker` which spawns `source` and `destination` docker, then transfers data in serialized form using `stdin` and `stdout`.

The `source` and `target` are converted into `DataSource` with their `Schema` and a `Mapper` is used to build the lineage.

Then each `AirbyteMessage` transfered from `source` to `destination` is accumulated to create stats for both `source` and `destination`.

## Support

### Airbyte 

Version: `0.40.4`.

### Sources:
- `airbyte/source-file`: `0.2.22`

### Destinations
- `airbyte/destination-csv`: `0.2.10`

## Requirements

Known working with the following tools:

- `java`: `18.0.2.1-tem` (installed with `sdkman`)
- `maven`: version `3.6.8`
- `docker`
- `docker-compose`

## Dependencies

- `asm`: `9.3`
- `kensu-java`: `1.1.1-SNAPSHOT` (TO BE FIXED -- requires to be built using `sbt`)

### Kensu java library (temp)
Buld SNAPSHOT locally for jackson 2.10

```
git clone git@github.com:kensuio/dam-client-java.git
cd dam-client-java
sdk use java 11.0.16-tem ## For SBT to work...
JACKSON_VERSION="2.10.0" sbt publishM2
```

## Build

### Configure

The configuration to publish the data observations is in the `resources` of the `airbyte-agent`, a file named `kensu.properties`.

This file contains several variable that will be loaded by the `KensuAgent`:
- `kensu.api`: if the observations must be published to a URI, this takes the based HTTPS URL -- default to Kensu Community Edition [https://community-api.kensuapp.com](https://community-api.kensuapp.com).
- `kensu.auth_token`: this is the authentorization token (JWT) to publish observations online.
- `kensu.offline.enabled`: if `true` the observations are saved in the below file.
- `kensu.offline.file`: where the observations will be written if the above is `true`.

> Update the `properties` before building this agent... or simply override the file on the classpath (requires update in the `Dockerfile` to include yours).

### Build jars and Docker

First make sure, you are in the `airbyte-do` folder you have cloned, then execute:

```sh
mvn clean install && docker build -t kensuio/airbyte-worker -f airbyte-instrumentation/Dockerfile .
```

The first part will generate:
- the `javaagent` uber-jar `airbyte-instrumentation` and its associated manifest
- the `airbyte-agent` uber-jar and the properties file -- shaded to avoid clash in airbyte-workers' classpath

The second part builds the docker image `kensuio/airbyte-worker` that adds both jars in the `airbyte/worker:0.40.4` script.

## Run

### Using Docker

During [Build](#Build), a new docker image was created on top of `airbyte/worker:0.40.4`, adding the `airbyte-agent` as `javaagent`.

So this image needs to be added to the `airbyte`'s `docker-compose` configuration, to do this first clone `airbyte`:

```sh
git clone git@github.com:airbytehq/airbyte.git
cd airbyte
```

Then update the `docker-compose.yaml`, locating `image: airbyte/worker:${VERSION}` and change it in `kensuio/airbyte-worker:latest`

> NOTE: if you want to active the java debug (for developers), then add after `environment`
> ```yaml
>     ports:
>      - 9044:9044 # java debug
>```

Then run
```sh
docker-compose up
```

## Test

Go to airbyte [http://localhost:8000/](http://localhost:8000/) and configure an online CSV source for [https://www.donneesquebec.ca/recherche/fr/dataset/857d007a-f195-434b-bc00-7012a6244a90/resource/16f55019-f05d-4375-a064-b75bce60543d/download/pf-mun-2019-2019.csv](https://www.donneesquebec.ca/recherche/fr/dataset/857d007a-f195-434b-bc00-7012a6244a90/resource/16f55019-f05d-4375-a064-b75bce60543d/download/pf-mun-2019-2019.csv) and add a local CSV as destination.

Execute the sync (push on the button).

If you configured `kensu.offline.enabled`:
- `true`: then enter in `workers` docker and go read the offline file created with observations
- `false`: check the Kensu UI to review the observations (metadata, metrics, lineage, ...).