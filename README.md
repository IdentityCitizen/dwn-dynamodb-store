# DWN SQL Stores <!-- omit in toc -->

[![NPM](https://img.shields.io/npm/v/@tbd54566975/dwn-sql-store.svg?style=flat-square&logo=npm&logoColor=FFFFFF&color=FFEC19&santize=true)](https://www.npmjs.com/package/@tbd54566975/dwn-sql-store)
[![Build Status](https://img.shields.io/github/actions/workflow/status/TBD54566975/dwn-sql-store/integrity-checks.yml?branch=main&logo=github&label=ci&logoColor=FFFFFF&style=flat-square)](https://github.com/TBD54566975/dwn-sql-store/actions/workflows/integrity-checks.yml)
[![Coverage](https://img.shields.io/codecov/c/gh/tbd54566975/dwn-sql-store/main?logo=codecov&logoColor=FFFFFF&style=flat-square&token=YI87CKF1LI)](https://codecov.io/github/TBD54566975/dwn-sql-store)
[![License](https://img.shields.io/npm/l/@tbd54566975/dwn-sql-store.svg?style=flat-square&color=24f2ff&logo=apache&logoColor=FFFFFF&santize=true)](https://github.com/TBD54566975/dwn-sql-store/blob/main/LICENSE)
[![Chat](https://img.shields.io/badge/chat-on%20discord-7289da.svg?style=flat-square&color=9a1aff&logo=discord&logoColor=FFFFFF&sanitize=true)](https://discord.com/channels/937858703112155166/969272658501976117)


SQL backed implementations of DWN `MessageStore`, `DataStore`, and `EventLog`. 

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
* DynamoDB ✔️

# Installation

```bash
npm install @tbd54566975/dwn-nosql-store
```

# Usage

## SQLite

```typescript
import Database from 'better-sqlite3';

import { Dwn } from '@tbd54566975/dwn-sdk-js'
import { SqliteDialect, MessageStoreSql, DataStoreSql, EventLogSql } from '@tbd54566975/dwn-sql-store';

const sqliteDialect = new SqliteDialect({
  database: async () => new Database('dwn.sqlite', {
    fileMustExist: true,
  })
});

const messageStore = new MessageStoreSql(sqliteDialect);
const dataStore = new DataStoreSql(sqliteDialect);
const eventLog = new EventLogSql(sqliteDialect);

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
This project uses serverless to run a local instance of dynamodb for testing.
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

| Environment Variable    | Description                                 |
| ----------------------- | ------------------------------------------- |
| `IS_OFFLINE`            | Uses a local DynamoDB instance for testing  |

