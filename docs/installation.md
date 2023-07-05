# How to Install ScalarDB Analytics with PostgreSQL in Your Local Environment by Using Docker

This document explains how to set up a local environment that runs ScalarDB Analytics with PostgreSQL using the multi-storage back-end of Cassandra, PostgreSQL, and DynamoDB local server using [Docker Compose](https://docs.docker.com/compose/).

## Prerequisites

- [Docker Engine](https://docs.docker.com/engine/) and [Docker Compose](https://docs.docker.com/compose/).

Follow the instructions on the Docker website according to your platform.

## Step 1. Clone the `scalardb-samples` repository

[scalardb-samples/scalardb-analytics-postgresql-sample](https://github.com/scalar-labs/scalardb-samples/tree/main/scalardb-analytics-postgresql-sample) repository is a project containing a sample configuration to set up ScalarDB Analytics with PostgreSQL.

Determine the location on your local machine where you want to run the scalardb-analytics-postgresql-sample app. Then, open Terminal, go to the location by using the `cd` command, and run the following commands:

```shell
$ git clone https://github.com/scalar-labs/scalardb-samples.git
$ cd scalardb-samples/scalardb-analytics-postgresql-sample
```

## Step 2. Log in to Docker

`docker login` is required to start the ScalarDB Analytics with PostgreSQL Docker image. Because the [scalardb-analytics-postgresql](https://github.com/orgs/scalar-labs/packages/container/package/scalardb-analytics-postgresql) repository in the GitHub Container Registry is currently private, your GitHub account needs permission to access the container images. To get permission for your account, please ask the person in charge of managing GitHub accounts in your organization. In addition, you will also need to use a personal access token (PAT) as a password to log in to `ghcr.io`. For more details, see the official documentation from GitHub at [Authenticating to the Container registry](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry#authenticating-to-the-container-registry).

```shell
# The read:packages scope in the personal access token settings must be selected to log in.
$ export CR_PAT=<YOUR_PERSONAL_ACCESS_TOKEN>
$ echo $CR_PAT | docker login ghcr.io -u <USERNAME> --password-stdin
```

## Step 3. Start up the ScalarDB Analytics with PostgreSQL services

The following command starts up the PostgreSQL instance that serves ScalarDB Analytics with PostgreSQL along with the back-end servers of Cassandra, PostgreSQL, and DynamoDB local in the Docker containers. When you first run the command, the required Docker images will be downloaded from the GitHub Container Registry.

```shell
$ docker-compose up
```

If you want to run the containers in the background, add the `-d` (--detach) option:

```shell
$ docker-compose up -d
```

If you already have your own ScalarDB database and want to use it as a back-end service, you can launch only the PostgreSQL instance without starting additional back-end servers in the container.

```shell
$ docker-compose up analytics
```

### Step 4. Run your analytical queries

Now, you should have all the required services running. To run analytical queries, see [Getting Started with ScalarDB Analytics with PostgreSQL](./getting-started.md).

### Step 5. Shut down the ScalarDB Analytics with PostgreSQL services

To shut down the containers, do one of the following in Terminal, depending on how you:

- If you started the containers in the foreground, press Ctrl+C where `docker-compose` is running.
- If you started the containers in the background, run the following command.

```shell
$ docker-compose down
```
