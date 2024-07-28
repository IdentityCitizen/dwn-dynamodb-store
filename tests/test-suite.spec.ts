import { ResumableTaskStoreNoSql } from '../src/resumable-task-store-nosql.js';
import { DataStoreNoSql } from '../src/data-store-nosql.js';
import { EventLogNoSql } from '../src/event-log-nosql.js';
import { MessageStoreNoSql } from '../src/message-store-nosql.js';
import { TestSuite } from '@tbd54566975/dwn-sdk-js/tests';
import { testDynamoDBDialect } from './test-dialects.js';

// Remove when we Node.js v18 is no longer supported by this project.
// Node.js v18 maintenance begins 2023-10-18 and is EoL 2025-04-30: https://github.com/nodejs/release#release-schedule
import { webcrypto } from 'node:crypto';
// @ts-expect-error ignore type mismatch
if (!globalThis.crypto) globalThis.crypto = webcrypto;

describe('NoSQL Store Test Suite', () => {

  // describe('DynamoDB Support', () => {
  //   TestSuite.runStoreDependentTests({
  //     messageStore       : new MessageStoreNoSql(testDynamoDBDialect),
  //     dataStore          : new DataStoreNoSql(testDynamoDBDialect),
  //     eventLog           : new EventLogSql(testDynamoDBDialect),
  //     resumableTaskStore : new ResumableTaskStoreNoSql(testDynamoDBDialect),
  //   });
  // });
  describe('DynamoDB Support', () => {
    TestSuite.runStoreDependentTests({
      // messageStore       : new MessageStoreNoSql(testDynamoDBDialect),
      // dataStore          : new DataStoreNoSql(testDynamoDBDialect),
      // eventLog           : new EventLogNoSql(testDynamoDBDialect),
      resumableTaskStore : new ResumableTaskStoreNoSql(testDynamoDBDialect),
    });
  });

});