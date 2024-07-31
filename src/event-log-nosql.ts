import type { EventLog, Filter, PaginationCursor } from '@tbd54566975/dwn-sdk-js';
import { Dialect } from './dialect/dialect.js';
import { extractTagsAndSanitizeIndexes } from './utils/sanitize-events.js';
import {
  marshall
} from '@aws-sdk/util-dynamodb'
import { 
  DynamoDBClient,
  ListTablesCommand,
  CreateTableCommand,
  AttributeDefinition,
  KeySchemaElement,
  BillingMode,
  TableClass,
  PutItemCommand,
  ScanCommand,
  DeleteItemCommand,
  ScanCommandInput,
  GlobalSecondaryIndex,
  BatchWriteItemCommand,
  BatchWriteItemCommandInput,
  UpdateItemCommand,
  ReturnValue,
  QueryCommandInput,
  QueryCommand
} from '@aws-sdk/client-dynamodb';

export class EventLogNoSql implements EventLog {
  #tableName: string = "eventLog";
  #client: DynamoDBClient;

  constructor(dialect: Dialect) {
    if ( process.env.IS_OFFLINE ) {
      this.#client = new DynamoDBClient({
        region: 'localhost',
        endpoint: 'http://0.0.0.0:8006',
        credentials: {
          accessKeyId: 'MockAccessKeyId',
          secretAccessKey: 'MockSecretAccessKey'
        },
      });
    } else {
       this.#client = new DynamoDBClient({
        region: 'ap-southeast-2'
      });
    }
  }

  async open(): Promise<void> {
    //console.log("Created client");

    const input = { // ListTablesInput
      //Limit: Number("1"),
    };
    const command = new ListTablesCommand(input);
    const response = await this.#client.send(command);
    //console.log(response);

    // Does table already exist?
    if ( response.TableNames ) {
      //console.log("Found Table Names in response");
      //console.log(response.TableNames);
      const tableExists = response.TableNames?.length > 0 && response.TableNames?.indexOf(this.#tableName) !== -1
      if ( tableExists ) {
        //console.log(this.#tableName + " TABLE ALREADY EXISTS");
      } else {
        //console.log("Trying to create table");
        const createTableInput = { // CreateTableInput
          AttributeDefinitions: [ // AttributeDefinitions // required
            { // AttributeDefinition
              AttributeName: "tenant", // required
              AttributeType: "S", // required
            } as AttributeDefinition,
            { // AttributeDefinition
              AttributeName: "watermark", // required
              AttributeType: "N", // required
            } as AttributeDefinition,
            { // AttributeDefinition
              AttributeName: "messageCid", // required
              AttributeType: "S", // required
            } as AttributeDefinition,
          ],
          TableName: this.#tableName, // required
          KeySchema: [ // KeySchema // required
            { // KeySchemaElement
              AttributeName: "tenant", // required
              KeyType: "HASH", // required
            } as KeySchemaElement,
            { // KeySchemaElement
              AttributeName: "messageCid", // required
              KeyType: "RANGE", // required
            } as KeySchemaElement,
          ],
          GlobalSecondaryIndexes: [
            {
                IndexName: "watermark",
                KeySchema: [
                    { AttributeName: "tenant", KeyType: 'HASH' } as KeySchemaElement, // GSI partition key
                    { AttributeName: "watermark", KeyType: 'RANGE' } as KeySchemaElement // Optional GSI sort key
                ],
                Projection: {
                    ProjectionType: 'ALL' // Adjust as needed ('ALL', 'KEYS_ONLY', 'INCLUDE')
                }
            } as GlobalSecondaryIndex
          ],
          BillingMode: "PAY_PER_REQUEST" as BillingMode,
          TableClass: "STANDARD" as TableClass,
        };

        //console.log("Create Table command");
        const createTableCommand = new CreateTableCommand(createTableInput);

        //console.log("Send table command");
        try {
          const createTableResponse = await this.#client.send(createTableCommand);
          //console.log(createTableResponse);
        } catch ( error ) {
          console.error(error);
        }
      }
    }
  }

  async close(): Promise<void> {
    this.#client.destroy();
  }

  async append(
    tenant: string,
    messageCid: string,
    indexes: Record<string, string | boolean | number>
  ): Promise<void> {
    if (!this.#client) {
      throw new Error(
        'Connection to database not open. Call `open` before using `append`.'
      );
    }

    // we execute the insert in a transaction as we are making multiple inserts into multiple tables.
    // if any of these inserts would throw, the whole transaction would be rolled back.
    // otherwise it is committed.
    //const putEventOperation = this.constructPutEventOperation({ tenant, messageCid, indexes });
    // Step 1: Increment the counter atomically
    try {
      const counterParams = {
        TableName: this.#tableName,
        Key: { "tenant": { S: tenant + "_counter"}, "messageCid": { S: "counter" } }, // Replace 'itemCounter' with your actual counter key
        UpdateExpression: 'SET #count = if_not_exists(#count, :start) + :incr',
        ExpressionAttributeNames: { '#count': 'count' },
        ExpressionAttributeValues: {
            ':incr': { N: '1' }, // Increment value
            ':start': { N: '0' } // Initial value if 'count' does not exist
        },
        ReturnValues: 'UPDATED_NEW' as ReturnValue
      };
  
      const updateCommand = new UpdateItemCommand(counterParams);
      const updateResult = await this.#client.send(updateCommand);
      const incNumber: string = updateResult.Attributes?.["count"]?.N ?? "";
      const incrementedCounter = parseInt(incNumber, 10);
      //console.log("Incremented Count: " + incrementedCounter);
      const { indexes: putIndexes, tags } = extractTagsAndSanitizeIndexes(indexes);
      //console.log(putIndexes);
      const fixIndexes = this.replaceReservedWords(putIndexes);
      const input = {
        "Item": {
          "tenant": {
            "S": tenant
          },
          "messageCid": {
            "S": messageCid
          },
          ...marshall(tags),
          ...marshall(fixIndexes),
          "watermark": { N: incrementedCounter.toString() }
        },
        "TableName": this.#tableName
      };

      //console.log(input);
      
      //console.log(input.Item.messageCid.S + " - " + input.Item.watermark.N);
      const command = new PutItemCommand(input);
      await this.#client.send(command);
    } catch ( error ) {
      console.error(error);
    }
  }

  // To avoid adding attributes which use reserved names, add an underscore prefix to indexes
  private replaceReservedWords(obj) {
    if (typeof obj !== 'object' || obj === null) {
        return obj; // Base case: return non-object values as-is
    }
    
    // Initialize an empty object to store the modified properties
    const newObj = {};
    
    // Iterate over each key-value pair in the object
    for (let key in obj) {
        if (obj.hasOwnProperty(key)) {
            // Construct new key with prefix only for top-level keys to prevent reserved dynamodb attribute names
            if ( key == "schema" ) {
              newObj["xschema"] = obj[key];
            } else if ( key == "method" ) {
              newObj["xmethod"] = obj[key];
            } else {
              newObj[key] = obj[key];
            }
        }
    }
    
    return newObj;
  }

  async getEvents(
    tenant: string,
    cursor?: PaginationCursor
  ): Promise<{events: string[], cursor?: PaginationCursor }> {

    // get events is simply a query without any filters. gets all events beyond the cursor.
    return this.queryEvents(tenant, [], cursor);
  }

  async queryEvents(
    tenant: string,
    filters: Filter[],
    cursor?: PaginationCursor
  ): Promise<{events: string[], cursor?: PaginationCursor }> {
    if (!this.#client) {
      throw new Error(
        'Connection to database not open. Call `open` before using `queryEvents`.'
      );
    }

    if ( filters ) {
      //console.log(JSON.stringify(filters, null, 2));
    }

    if ( cursor ) {
      //console.log(JSON.stringify(cursor, null, 2));
    }

    try {

      const filterDynamoDB: any = [];
      const expressionAttributeValues = {};

      for (const filter of filters) {
        const constructFilter = {
          FilterExpression: "",
        }
        const conditions: string[] = [];
        for ( const keyRaw in filter ) {
          // schema and method are reserved keywords so we replace them here
          const key = keyRaw == "schema" ? "xschema" : keyRaw == "method" ? "xmethod" : keyRaw;
          constructFilter.FilterExpression += key;
          const value = filter[key];
          if (typeof value === 'object') {
            if (value["gt"]) {
              conditions.push(key + " > :x" + key + "GT");
              expressionAttributeValues[":x" + key + "GT"] = value["gt"]
            }
            if (value["gte"]) {
              conditions.push(key + " >= :x" + key + "GTE");
              expressionAttributeValues[":x" + key + "GTE"] = value["gte"]
            }
            if (value["lt"]) {
              conditions.push(key + " < :x" + key + "LT");
              expressionAttributeValues[":x" + key + "LT"] = value["lt"]
            }
            if (value["lte"]) {
              conditions.push(key + " <= :x" + key + "LTE");
              expressionAttributeValues[":x" + key + "LTE"] = value["lte"]
            }
          } else {
            conditions.push(key + " = :x" + key + "EQ");
            expressionAttributeValues[":x" + key + "EQ"] = filter[keyRaw].toString();
          }
        }
        filterDynamoDB.push("(" + conditions.join(" AND ") + ")");
      }

      expressionAttributeValues[':tenant'] = tenant;

      //console.log(filterDynamoDB.join(" OR "));
      //console.log(expressionAttributeValues);

      const filterExp = filterDynamoDB.join(" OR ");

      const params: QueryCommandInput = {
        TableName: this.#tableName,
        IndexName: "watermark",
        KeyConditionExpression: '#tenant = :tenant',
        ExpressionAttributeNames: {
            '#tenant': "tenant"
        },
        ExpressionAttributeValues: marshall(expressionAttributeValues),
        ScanIndexForward: true,
      };

      if ( filterExp ) {
        params.FilterExpression = filterExp;
      }

      if ( cursor ) {
        params["ExclusiveStartKey"] = JSON.parse(cursor.messageCid);
      }

      //console.log(params);

      const command = new QueryCommand(params);
      const data = await this.#client.send(command);

      if( data.Items ){
        const events: string[] = [];
        const lastMessage: any = data.Items.at(-1);
        const cursorValue = {};
        if ( lastMessage !== undefined ) {
          cursorValue["tenant"] = lastMessage["tenant"];
          cursorValue["messageCid"] = lastMessage["messageCid"];
          cursorValue["watermark"] = lastMessage["watermark"];
          cursor = { messageCid: JSON.stringify(cursorValue), value: JSON.stringify(cursorValue) };
        }
        

        for (let { messageCid, watermark } of data.Items) {
          //console.log(messageCid?.S + " - " + watermark?.N);
            events.push(messageCid?.S ?? "");
        }
        //console.log("Returning: ");
        //console.log(events);
        return { events, cursor };
      }
        

      // let query = this.#db
      //   .selectFrom('eventLogMessages')
      //   .leftJoin('eventLogRecordsTags', 'eventLogRecordsTags.eventWatermark', 'eventLogMessages.watermark')
      //   .select('messageCid')
      //   .distinct()
      //   .select('watermark')
      //   .where('tenant', '=', tenant);

      // if (filters.length > 0) {
      //   // filter sanitization takes place within `filterSelectQuery`
      //   query = filterSelectQuery(filters, query);
      // }

      // if(cursor !== undefined) {
      //   // eventLogMessages in the sql store uses the watermark cursor value which is a number in SQL
      //   // if not we will return empty results
      //   const cursorValue = cursor.value as number;
      //   const cursorMessageCid = cursor.messageCid;

      //   query = query.where(({ eb, refTuple, tuple }) => {
      //     // https://kysely-org.github.io/kysely-apidoc/interfaces/ExpressionBuilder.html#refTuple
      //     return eb(refTuple('watermark', 'messageCid'), '>', tuple(cursorValue, cursorMessageCid));
      //   });
      // }

      // query = query.orderBy('watermark', 'asc').orderBy('messageCid', 'asc');

      // const events: string[] = [];
      // // we always return a cursor with the event log query, so we set the return cursor to the properties of the last item.
      // let returnCursor: PaginationCursor | undefined;
      // if (this.#dialect.isStreamingSupported) {
      //   for await (let { messageCid, watermark: value } of query.stream()) {
      //     events.push(messageCid);
      //     returnCursor = { messageCid, value };
      //   }
      // } else {
      //   const results = await query.execute();
      //   for (let { messageCid, watermark: value } of results) {
      //     events.push(messageCid);
      //     returnCursor = { messageCid, value };
      //   }
      // }

    } catch (error) {
      console.error(error);
    }

    // return { events, cursor: returnCursor };
    return { events: [], cursor: {messageCid: "123", value: 1} };
  }

  async deleteEventsByCid(
    tenant: string,
    messageCids: Array<string>
  ): Promise<void> {
    if (!this.#client) {
      throw new Error(
        'Connection to database not open. Call `open` before using `delete`.'
      );
    }

    if (messageCids.length === 0) {
      return;
    }

    const keysToDelete: any = [
    ];

    for ( const messageCid of messageCids ) {
      keysToDelete.push( { "tenant": { S: tenant}, "messageCid": { S: messageCid} } );
    }

    await this.deleteItems(keysToDelete)

  }

  async deleteItems(keysToDelete: { [key: string]: any }[]) {

    // Prepare requests in batches of 25 (DynamoDB batchWriteItem limit)
    const batchSize = 25;
    const batches: BatchWriteItemCommandInput[] = [];

    for (let i = 0; i < keysToDelete.length; i += batchSize) {
        const batchKeys = keysToDelete.slice(i, i + batchSize);

        const deleteRequests = batchKeys.map(key => ({
            DeleteRequest: {
                Key: key
            }
        }));

        batches.push({
            RequestItems: {
                [this.#tableName]: deleteRequests
            }
        });
        //console.log(JSON.stringify(batches));
    }

    // Execute batches using batchWriteItem
    for (const batch of batches) {
      //console.log(JSON.stringify(batch));
      const command = new BatchWriteItemCommand(batch);
      try {
        
          const response = await this.#client.send(command);
          //console.log('Batch delete successful:', response);
      } catch (error) {
          console.error('Error deleting batch:', error);
          // Handle error as needed
      }
    }
}

  async clear(): Promise<void> {
    if (!this.#client) {
      throw new Error(
        'Connection to database not open. Call `open` before using `clear`.'
      );
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
                  Key: {
                      'tenant': { S: item.tenant.S.toString() }, // Adjust 'primaryKey' based on your table's partition key
                      'messageCid': { S: item.messageCid.S.toString() }
                  }
              };
              //console.log(deleteParams);
              let deleteCommand = new DeleteItemCommand(deleteParams);
              await this.#client.send(deleteCommand);
              //console.log("Deleted item successfully");
          }

          // Continue scanning if we have more items
          scanParams.ExclusiveStartKey = scanResult.LastEvaluatedKey;

      } while (scanResult.LastEvaluatedKey);

      //console.log(`Successfully cleared all data from "dataStore"`);
    } catch (err) {
        console.error('Unable to clear table:', err);
    }
  }
}