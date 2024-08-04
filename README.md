# DWN AWS DynamoDB Store <!-- omit in toc -->


DynamoDB NoSQL backed implementations of DWN `MessageStore`, `DataStore`, `EventLog` and `ResumableTask`. 

- [Supported DBs](#supported-dbs)
- [Installation](#installation)
- [Usage](#usage)
  - [SQLite](#sqlite)
  - [MySQL](#mysql)
  - [PostgreSQL](#postgresql)
- [Development](#development)
  - [Prerequisites](#prerequisites)
    - [`node` and `npm`](#node-and-npm)
  - [Running Tests](#running-tests)
  - [`npm` scripts](#npm-scripts)
  - [Environment Variables](#environment-variables)


# Supported DBs
* AWS DynamoDB ✔️
* AWS DynamoDB Local ✔️

# Installation

```bash
npm install @tbd54566975/dwn-dynamodb-store
```

# Considerations

- Since DynamoDB is an AWS hosted service, factor latency into your application requirements. Use the `REGION` environment variable to use an AWS Region closest to where your application runs.

# Usage

## DynamoDB

```typescript
import Database from 'better-sqlite3';

import { Dwn } from '@tbd54566975/dwn-sdk-js'
import { MessageStoreNoSql, DataStoreNoSql, EventLogNoSql } from '@tbd54566975/dwn-sql-store';

const messageStore = new MessageStoreNoSql({});
const dataStore = new DataStoreNoSql({});
const eventLog = new EventLogNoSql({});

const dwn = await Dwn.create({ messageStore, dataStore, eventLog });
```

# Development

## Prerequisites
### `node` and `npm`
This project is developed and tested with [Node.js](https://nodejs.org/en/about/previous-releases)
`v18` and `v20` and NPM `v9`. You can verify your `node` and `npm` installation via the terminal:

```
$ node --version
v20.3.0
$ npm --version
9.6.7
```

### `serverless`
This project uses serverless to run a local instance of dynamodb for testing purposes only.
```
npm i serverless -g
```

If you don't have `node` installed. Feel free to choose whichever approach you feel the most comfortable with. If you don't have a preferred installation method, i'd recommend using `nvm` (aka node version manager). `nvm` allows you to install and use different versions of node. It can be installed by running `brew install nvm` (assuming that you have homebrew)

Once you have installed `nvm`, install the desired node version with `nvm install vX.Y.Z`.

## Running Tests
> 💡 Make sure you have all the [prerequisites](#prerequisites)

0. clone the repo and `cd` into the project directory
1. Install all project dependencies by running `npm install`
2. Start the test databases using `./scripts/start-databases`
3. Open a second terminal and run tests using `npm run test` (serverless struggles with daemons so it requires a dedicated terminal)

`Ctrl+C` in the terminal where you started the database when tests have completed.

## `npm` scripts

| Script                  | Description                                 |
| ----------------------- | ------------------------------------------- |
| `npm run build:cjs`     | compiles typescript into CommonJS           |
| `npm run build:esm`     | compiles typescript into ESM JS             |
| `npm run build`         | compiles typescript into ESM JS & CommonJS  |
| `npm run clean`         | deletes compiled JS                         |
| `npm run test`          | runs tests.                                 |
| `npm run test-coverage` | runs tests and includes coverage            |
| `npm run lint`          | runs linter                                 |
| `npm run lint:fix`      | runs linter and fixes auto-fixable problems |

## Environment Variables

| Environment Variable    | Value          | Description                                 |
| ----------------------- | -------------- | ------------------------------------------- |
| `IS_OFFLINE`            | true|false     | Uses a local DynamoDB instance for testing  |
| `AWS_REGION`            | ap-southeast-2 | The region where the DynamoDB tables should be created (https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.RegionsAndAvailabilityZones.html) |


