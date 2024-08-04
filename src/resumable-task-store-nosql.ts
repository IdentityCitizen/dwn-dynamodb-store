import { DwnDatabaseType } from './types.js';
import { Dialect } from './dialect/dialect.js';
import {
  DynamoDBClient,
  ListTablesCommand,
  CreateTableCommand,
  AttributeDefinition,
  KeySchemaElement,
  BillingMode,
  TableClass,
  GetItemCommand,
  PutItemCommand,
  ScanCommand,
  DeleteItemCommand,
  ScanCommandInput,
  QueryCommandInput,
  GlobalSecondaryIndex,
  QueryCommand
} from '@aws-sdk/client-dynamodb';
import {
  marshall
} from '@aws-sdk/util-dynamodb';
import { Cid, ManagedResumableTask, ResumableTaskStore } from '@tbd54566975/dwn-sdk-js';

export class ResumableTaskStoreNoSql implements ResumableTaskStore {
  #tableName = 'resumableTasks';
  #tenantid = 'default'; // Used as a hash key for when we need to query based on timeout
  private static readonly taskTimeoutInSeconds = 60;

  #client: DynamoDBClient;

  constructor(dialect: Dialect) {
    if ( process.env.IS_OFFLINE == 'true' ) {
      this.#client = new DynamoDBClient({
        region      : 'localhost',
        endpoint    : 'http://0.0.0.0:8006',
        credentials : {
          accessKeyId     : 'MockAccessKeyId',
          secretAccessKey : 'MockSecretAccessKey'
        },
      });
    } else {
      this.#client = new DynamoDBClient({
        region: process.env.AWS_REGION ? process.env.AWS_REGION : 'ap-southeast-2'
      });
    }
  }

  async open(): Promise<void> {
    const input = { // ListTablesInput
    };
    const command = new ListTablesCommand(input);
    const response = await this.#client.send(command);

    // Does table already exist?
    if ( response.TableNames ) {

      const tableExists = response.TableNames?.length > 0 && response.TableNames?.indexOf(this.#tableName) !== -1;
      if ( tableExists ) {
        return;
      }
    }


    const createTableInput = { // CreateTableInput
      AttributeDefinitions: [ // AttributeDefinitions // required
        { // AttributeDefinition
          AttributeName : 'tenantid', // required
          AttributeType : 'S', // required
        } as AttributeDefinition,
        { // AttributeDefinition
          AttributeName : 'taskid', // required
          AttributeType : 'S', // required
        } as AttributeDefinition,
        { // AttributeDefinition
          AttributeName : 'timeout', // required
          AttributeType : 'N', // required
        } as AttributeDefinition,
      ],
      TableName : this.#tableName, // required
      KeySchema : [ // KeySchema // required
        { // KeySchemaElement
          AttributeName : 'taskid', // required
          KeyType       : 'HASH', // required
        } as KeySchemaElement,
      ],
      GlobalSecondaryIndexes: [
        {
          IndexName : 'timeout',
          KeySchema : [
                { AttributeName: 'tenantid', KeyType: 'HASH' } as KeySchemaElement, // GSI partition key
                { AttributeName: 'timeout', KeyType: 'RANGE' } as KeySchemaElement // Optional GSI sort key
          ],
          Projection: {
            ProjectionType: 'ALL' // Adjust as needed ('ALL', 'KEYS_ONLY', 'INCLUDE')
          }
        } as GlobalSecondaryIndex
      ],
      BillingMode : 'PAY_PER_REQUEST' as BillingMode,
      TableClass  : 'STANDARD' as TableClass,
    };

    const createTableCommand = new CreateTableCommand(createTableInput);

    try {
      await this.#client.send(createTableCommand);
    } catch ( error ) {
      console.error(error);
    }

  }

  async close(): Promise<void> {
    this.#client.destroy();
  }

  async register(task: any, timeoutInSeconds: number): Promise<ManagedResumableTask> {
    if (!this.#client) {
      throw new Error('Connection to database not open. Call `open` before using `register`.');
    }

    const id = await Cid.computeCid(task);
    const timeout = Date.now() + timeoutInSeconds * 1000;
    const taskString = JSON.stringify(task);
    const retryCount = 0;
    //const taskEntryInDatabase: ManagedResumableTask = { id, task: taskString, timeout, retryCount };

    const input = {
      'Item': {
        'taskid': {
          'S': id
        },
        'tenantid': {
          'S': this.#tenantid
        },
        'timeout': {
          N: timeout.toString()
        },
        'task': {
          'S': taskString
        },
        'retryCount': {
          'S': retryCount.toString()
        }
      },
      'TableName': this.#tableName
    };
    const command = new PutItemCommand(input);
    await this.#client.send(command);

    return {
      id,
      task,
      retryCount,
      timeout,
    };
  }

  async grab(count: number): Promise<ManagedResumableTask[]> {
    if (!this.#client) {
      throw new Error('Connection to database not open. Call `open` before using `grab`.');
    }
    try {

      const now = Date.now();
      const newTimeout = now + (ResumableTaskStoreNoSql.taskTimeoutInSeconds * 1000);

      const params: QueryCommandInput = {
        TableName                : this.#tableName,
        IndexName                : 'timeout',
        KeyConditionExpression   : '#tenantid = :tenantid AND #timeout <= :timeout',
        ExpressionAttributeNames : {
          '#tenantid' : 'tenantid',
          '#timeout'  : 'timeout'
        },
        ExpressionAttributeValues: marshall({
          ':tenantid' : this.#tenantid,
          ':timeout'  : now
        }),
        ScanIndexForward : true,
        Limit            : count
      };

      const command = new QueryCommand(params);
      const data = await this.#client.send(command);

      if ( data.Items ) {
        for (let item of data.Items) {
          if ( item.taskid.S !== undefined ) { // will always be valued as it's an index
            await this.delete(item.taskid.S);
            // recreate object with updated value
            const input = {
              'Item': {
                'taskid'   : item.taskid,
                'tenantid' : {
                  'S': this.#tenantid
                },
                'timeout': {
                  N: newTimeout.toString()
                },
                'task'       : item.task,
                'retryCount' : item.retryCount
              },
              'TableName': this.#tableName
            };
            const command = new PutItemCommand(input);
            await this.#client.send(command);
          }
        }
      }

      const tasksToReturn = data.Items?.map((task) => {
        return {
          id         : task.taskid.S ?? '',
          task       : JSON.parse(task.task.S ?? '{}'),
          retryCount : parseInt(task.retryCount.N ?? '0'),
          timeout    : newTimeout,
        };
      });
      let tasks: DwnDatabaseType['resumableTasks'][] = tasksToReturn ?? [];
      return tasks;
    } catch ( error ) {
      console.error(error);
      let tasks: DwnDatabaseType['resumableTasks'][] = [];
      return tasks;
    }
  }

  async read(taskId: string): Promise<ManagedResumableTask | undefined> {
    if (!this.#client) {
      throw new Error('Connection to database not open. Call `open` before using `read`.');
    }

    const input = { // GetItemInput
      TableName : this.#tableName, // required
      Key       : { // Key // required
        'taskid': { // AttributeValue Union: only one key present
          S: taskId,
        }
      },
      AttributesToGet: [ // AttributeNameList
        'taskid', 'task', 'timeout', 'retryCount'
      ]
    };
    const command = new GetItemCommand(input);
    const response = await this.#client.send(command);

    if ( !response.Item ) {
      return undefined;
    }
    return {
      id         : response.Item.taskid.S ?? '',
      task       : response.Item.task.S ?? '',
      timeout    : parseInt(response.Item.timeout.N ?? '0'),
      retryCount : parseInt(response.Item.retryCount.N ?? '0')
    };
  }

  async extend(taskId: string, timeoutInSeconds: number): Promise<void> {
    try {
      if (!this.#client) {
        throw new Error('Connection to database not open. Call `open` before using `extend`.');
      }

      const timeout = Date.now() + (timeoutInSeconds * 1000);

      const input = { // GetItemInput
        TableName : this.#tableName, // required
        Key       : { // Key // required
          'taskid': { // AttributeValue Union: only one key present
            S: taskId,
          }
        }
      };
      const command = new GetItemCommand(input);
      const response = await this.#client.send(command);

      if ( !response.Item ) {
        return;
      }

      response.Item.timeout = {
        N: timeout.toString()
      };

      const inputRecreate = {
        'Item': {
          'taskid'   : response.Item.taskid,
          'tenantid' : {
            'S': this.#tenantid
          },
          'timeout': {
            N: timeout.toString()
          },
          'task'       : response.Item.task,
          'retryCount' : response.Item.retryCount
        },
        'TableName': this.#tableName
      };
      const commandInput = new PutItemCommand(inputRecreate);
      await this.#client.send(commandInput);
    } catch ( error ) {
    }
  }

  async delete(taskId: string): Promise<void> {
    if (!this.#client) {
      throw new Error('Connection to database not open. Call `open` before using `delete`.');
    }

    let deleteParams = {
      TableName : this.#tableName,
      Key       : marshall({
        'taskid': taskId, // Adjust 'primaryKey' based on your table's partition key
      })
    };

    let deleteCommand = new DeleteItemCommand(deleteParams);
    await this.#client.send(deleteCommand);
  }

  async clear(): Promise<void> {
    if (!this.#client) {
      throw new Error('Connection to database not open. Call `open` before using `clear`.');
    }

    try {
      let scanParams: ScanCommandInput = {
        TableName: this.#tableName
      };

      let scanCommand = new ScanCommand(scanParams);
      let scanResult;

      do {
        scanResult = await this.#client.send(scanCommand);

        // Delete each item
        for (let item of scanResult.Items) {
          let deleteParams = {
            TableName : this.#tableName,
            Key       : marshall({
              'taskid': item.taskid.S.toString()
            })
          };

          let deleteCommand = new DeleteItemCommand(deleteParams);
          await this.#client.send(deleteCommand);
        }

        // Continue scanning if we have more items
        scanParams.ExclusiveStartKey = scanResult.LastEvaluatedKey;

      } while (scanResult.LastEvaluatedKey);

    } catch (err) {
      console.error('Unable to clear table:', err);
    }
  }
}
