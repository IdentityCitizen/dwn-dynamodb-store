import { DwnDatabaseType } from './types.js';
import { Dialect } from './dialect/dialect.js';
import { executeWithRetryIfDatabaseIsLocked } from './utils/transaction.js';
import { Kysely } from 'kysely';
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
  ScanCommandOutput,
  QueryCommandInput,
  GlobalSecondaryIndex,
  QueryCommand
} from '@aws-sdk/client-dynamodb';
import {
  marshall
} from '@aws-sdk/util-dynamodb'
import { Cid, ManagedResumableTask, ResumableTaskStore } from '@tbd54566975/dwn-sdk-js';

export class ResumableTaskStoreNoSql implements ResumableTaskStore {
  #tableName: string = "resumableTasks";
  #tenantid: string = "default"; // Used as a hash key for when we need to query based on timeout
  private static readonly taskTimeoutInSeconds = 60;

  #client: DynamoDBClient;
  

  constructor(dialect: Dialect) {
    this.#client = new DynamoDBClient({
      region: 'localhost',
      endpoint: 'http://0.0.0.0:8006',
      credentials: {
        accessKeyId: 'MockAccessKeyId',
        secretAccessKey: 'MockSecretAccessKey'
      },
    });
  }

  async open(): Promise<void> {
    console.log("Start open");
    const input = { // ListTablesInput
    };
    const command = new ListTablesCommand(input);
    const response = await this.#client.send(command);
    console.log(response);

    // Does table already exist?
    if ( response.TableNames ) {
      console.log("Found Table Names in response");

      const tableExists = response.TableNames?.length > 0 && response.TableNames?.indexOf(this.#tableName) !== -1
      if ( tableExists ) {
        console.log("TABLE ALREADY EXISTS");
        return;
      }
    }

    console.log("Trying to create table");

    const createTableInput = { // CreateTableInput
      AttributeDefinitions: [ // AttributeDefinitions // required
        { // AttributeDefinition
          AttributeName: "tenantid", // required
          AttributeType: "S", // required
        } as AttributeDefinition,
        { // AttributeDefinition
          AttributeName: "taskid", // required
          AttributeType: "S", // required
        } as AttributeDefinition,
        { // AttributeDefinition
          AttributeName: "timeout", // required
          AttributeType: "N", // required
        } as AttributeDefinition,
      ],
      TableName: this.#tableName, // required
      KeySchema: [ // KeySchema // required
        { // KeySchemaElement
          AttributeName: "taskid", // required
          KeyType: "HASH", // required
        } as KeySchemaElement,
      ],
      GlobalSecondaryIndexes: [
        {
            IndexName: "timeout",
            KeySchema: [
                { AttributeName: "tenantid", KeyType: 'HASH' } as KeySchemaElement, // GSI partition key
                { AttributeName: "timeout", KeyType: 'RANGE' } as KeySchemaElement // Optional GSI sort key
            ],
            Projection: {
                ProjectionType: 'ALL' // Adjust as needed ('ALL', 'KEYS_ONLY', 'INCLUDE')
            }
        } as GlobalSecondaryIndex
      ],
      BillingMode: "PAY_PER_REQUEST" as BillingMode,
      TableClass: "STANDARD" as TableClass,
    };
    console.log(createTableInput);

    console.log("Create Table command");
    const createTableCommand = new CreateTableCommand(createTableInput);

    console.log("Send table command");
    try {
      const createTableResponse = await this.#client.send(createTableCommand);
      console.log(createTableResponse);
    } catch ( error ) {
      console.error(error);
    }
    // if (this.#db) {
    //   return;
    // }

    // this.#db = new Kysely<DwnDatabaseType>({ dialect: this.#dialect });

    // let table = this.#db.schema
    //   .createTable('resumableTasks')
    //   .ifNotExists()
    //   .addColumn('id', 'varchar(255)', (col) => col.primaryKey())
    //   .addColumn('task', 'text')
    //   .addColumn('timeout', 'integer')
    //   .addColumn('retryCount', 'integer');

    // await table.execute();

    // this.#db.schema
    //   .createIndex('index_timeout')
    //   .ifNotExists()
    //   .on('resumableTasks')
    //   .column('timeout')
    //   .execute();
  }

  async close(): Promise<void> {
    console.log("Start close");
    this.#client.destroy();
  }

  async register(task: any, timeoutInSeconds: number): Promise<ManagedResumableTask> {
    console.log("Start register");
    if (!this.#client) {
      throw new Error('Connection to database not open. Call `open` before using `register`.');
    }

    const id = await Cid.computeCid(task);
    const timeout = Date.now() + timeoutInSeconds * 1000;
    const taskString = JSON.stringify(task);
    const retryCount = 0;
    //const taskEntryInDatabase: ManagedResumableTask = { id, task: taskString, timeout, retryCount };

    const input = {
      "Item": {
        "taskid": {
          "S": id
        },
        "tenantid": {
          "S": this.#tenantid
        },
        "timeout": {
          N: timeout.toString()
        },
        "task": {
          "S": taskString
        },
        "retryCount": {
          "S": retryCount.toString()
        }
      },
      "TableName": this.#tableName
    };
    console.log(input);
    const command = new PutItemCommand(input);
    await this.#client.send(command);

    // if (!this.#db) {
    //   throw new Error('Connection to database not open. Call `open` before using `register`.');
    // }

    // const id = await Cid.computeCid(task);
    // const timeout = Date.now() + timeoutInSeconds * 1000;
    // const taskString = JSON.stringify(task);
    // const retryCount = 0;
    // const taskEntryInDatabase: ManagedResumableTask = { id, task: taskString, timeout, retryCount };
    // await this.#db.insertInto('resumableTasks').values(taskEntryInDatabase).execute();

    return {
      id,
      task,
      retryCount,
      timeout,
    };
  }

  async grab(count: number): Promise<ManagedResumableTask[]> {
    console.log("Start grab");
    if (!this.#client) {
      throw new Error('Connection to database not open. Call `open` before using `grab`.');
    }
    try {

      const now = Date.now();
      const newTimeout = now + (ResumableTaskStoreNoSql.taskTimeoutInSeconds * 1000);

      const params: QueryCommandInput = {
        TableName: this.#tableName,
        IndexName: "timeout",
        KeyConditionExpression: '#tenantid = :tenantid AND #timeout <= :timeout',
        ExpressionAttributeNames: {
            "#tenantid": "tenantid",
            "#timeout": "timeout"
        },
        ExpressionAttributeValues: marshall({
          ":tenantid": this.#tenantid,
          ":timeout": now
        }),
        ScanIndexForward: true,
        Limit: count
      };
      console.log("Grab Input");
      console.log(params);

      const command = new QueryCommand(params);
      const data = await this.#client.send(command);
      console.log(data);

      if ( data.Items ) {
        for (let item of data.Items) {
          if ( item.taskid.S !== undefined ) { // will always be valued as it's an index
            console.log("Attempting delete of " + item.taskid.S);
            await this.delete(item.taskid.S);
            // recreate object with updated value
            const input = {
              "Item": {
                "taskid": item.taskid,
                "tenantid": {
                  "S": this.#tenantid
                },
                "timeout": {
                  N: newTimeout.toString()
                },
                "task": item.task,
                "retryCount": item.retryCount
              },
              "TableName": this.#tableName
            };
            console.log("Recreate");
            console.log(input);
            const command = new PutItemCommand(input);
            await this.#client.send(command);
          }
        }
      }

      // const operation = async (transaction) => {
      //   tasks = await transaction
      //     .selectFrom('resumableTasks')
      //     .selectAll()
      //     .where('timeout', '<=', now)
      //     .limit(count)
      //     .execute();

      //   if (tasks.length > 0) {
      //     const ids = tasks.map((task) => task.id);
      //     await transaction
      //       .updateTable('resumableTasks')
      //       .set({ timeout: newTimeout })
      //       .where((eb) => eb('id', 'in', ids))
      //       .execute();
      //   }
      // };

      // await executeWithRetryIfDatabaseIsLocked(this.#db, operation);

      // const tasksToReturn = tasks.map((task) => {
      //   return {
      //     id         : task.id,
      //     task       : JSON.parse(task.task),
      //     retryCount : task.retryCount,
      //     timeout    : task.timeout,
      //   };
      // });
      const tasksToReturn = data.Items?.map((task) => {
        return {
          id         : task.taskid.S ?? "",
          task       : JSON.parse(task.task.S ?? "{}"),
          retryCount : parseInt(task.retryCount.N ?? "0"),
          timeout    : newTimeout,
        };
      });
      console.log(tasksToReturn);
      let tasks: DwnDatabaseType['resumableTasks'][] = tasksToReturn ?? [];
      console.log("Populating");
      console.log(JSON.stringify(tasksToReturn, null, 2));
      return tasks;
    } catch ( error ) {
      console.error(error);
      let tasks: DwnDatabaseType['resumableTasks'][] = [];
      return tasks;
    }
  }

  async read(taskId: string): Promise<ManagedResumableTask | undefined> {
    console.log("Start read");
    if (!this.#client) {
      throw new Error('Connection to database not open. Call `open` before using `read`.');
    }

    const input = { // GetItemInput
      TableName: this.#tableName, // required
      Key: { // Key // required
        "taskid": { // AttributeValue Union: only one key present
          S: taskId,
        }
      },
      AttributesToGet: [ // AttributeNameList
        "taskid", "task", "timeout", "retryCount"
      ]
    };
    console.log(input);
    const command = new GetItemCommand(input);
    const response = await this.#client.send(command);

    if ( !response.Item ) {
      return undefined;
    }
    console.log("Read Response:");
    console.log(response.Item);
    return {
        id: response.Item.taskid.S ?? "",
        task: response.Item.task.S ?? "",
        timeout: parseInt(response.Item.timeout.N ?? "0"),
        retryCount: parseInt(response.Item.retryCount.N ?? "0")
    }

    // return this.#db
    //   .selectFrom('resumableTasks')
    //   .selectAll()
    //   .where('id', '=', taskId)
    //   .executeTakeFirst();
  }

  async extend(taskId: string, timeoutInSeconds: number): Promise<void> {
    console.log("Start extend");
    if (!this.#client) {
      throw new Error('Connection to database not open. Call `open` before using `extend`.');
    }

    const timeout = Date.now() + (timeoutInSeconds * 1000);

    const input = { // GetItemInput
      TableName: this.#tableName, // required
      Key: { // Key // required
        "taskid": { // AttributeValue Union: only one key present
          S: taskId,
        }
      }
    };
    console.log("Extend Input Task:");
    console.log(input);
    const command = new GetItemCommand(input);
    const response = await this.#client.send(command);

    if ( !response.Item ) {
      return;
    }

    response.Item.timeout = {
      N: timeout.toString()
    }

    const inputRecreate = {
      "Item": {
        "taskid": response.Item.taskid,
        "tenantid": {
          "S": this.#tenantid
        },
        "timeout": {
          N: timeout.toString()
        },
        "task": response.Item.task,
        "retryCount": response.Item.retryCount
      },
      "TableName": this.#tableName
    };
    console.log("Extend Input");
    console.log(inputRecreate);
    const commandInput = new PutItemCommand(inputRecreate);
    await this.#client.send(command);



    // await this.#db
    //   .updateTable('resumableTasks')
    //   .set({ timeout })
    //   .where('id', '=', taskId)
    //   .execute();
  }

  async delete(taskId: string): Promise<void> {
    console.log("Start delete");
    if (!this.#client) {
      throw new Error('Connection to database not open. Call `open` before using `delete`.');
    }

    let deleteParams = {
      TableName: this.#tableName,
      Key: marshall({
          'taskid': taskId, // Adjust 'primaryKey' based on your table's partition key
      })
    };
    
    let deleteCommand = new DeleteItemCommand(deleteParams);
    await this.#client.send(deleteCommand);

    // await this.#db
    //   .deleteFrom('resumableTasks')
    //   .where('id', '=', taskId)
    //   .execute();
  }

  async clear(): Promise<void> {
    console.log("Start clear");
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
                  TableName: this.#tableName,
                  Key: marshall({
                      'taskid': item.taskid.S.toString()
                  })
              };
              
              let deleteCommand = new DeleteItemCommand(deleteParams);
              await this.#client.send(deleteCommand);
              console.log("Deleted item successfully");
          }

          // Continue scanning if we have more items
          scanParams.ExclusiveStartKey = scanResult.LastEvaluatedKey;

      } while (scanResult.LastEvaluatedKey);

      console.log(`Successfully cleared all data from this.#tableName`);
    } catch (err) {
        console.error('Unable to clear table:', err);
    }
  }
}
