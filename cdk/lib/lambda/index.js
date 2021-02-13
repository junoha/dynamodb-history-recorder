/*
Stream event format
{
  "invocationId": "510a881f-8940-4933-9b1f-2c269240a87a",
  "sourceKinesisStreamArn": "arn:aws:kinesis:<region-name>:<account-id>:stream/<ddb-table-name>",
  "deliveryStreamArn": "arn:aws:firehose:<region-name>:<account-id>:deliverystream/<ddb-table-name>",
  "region": "<region-name>",
  "records": [
    {
      "recordId": "49615332175504399275388999519764255559777264071916650498000000",
      "approximateArrivalTimestamp": 1613022603017,
      "data": "...base64...",
      "kinesisRecordMetadata": {
        "sequenceNumber": "49615332175504399275388999519764255559777264071916650498",
        "subsequenceNumber": 0,
        "partitionKey": "0C37C3E39761B8F376F019B2572B8081",
        "shardId": "shardId-000000000000",
        "approximateArrivalTimestamp": 1613022603017
      }
    }
  ]
}

base64 decoded records[].data
{
  "awsRegion": "<region-name>",
  "dynamodb": {
    "ApproximateCreationDateTime": 1613022644643,
    "Keys": {
      "k1": {
        "S": "cea899cd-edd5-44f0-aca8-f2ab0e52f5b9"
      }
    },
    "NewImage": {
      "product": {
        "S": "Gloves aaaa"
      },
      "active": {
        "BOOL": false
      },
      "k1": {
        "S": "cea899cd-edd5-44f0-aca8-f2ab0e52f5b9"
      }
    },
    "OldImage": {
      "product": {
        "S": "Gloves"
      },
      "active": {
        "BOOL": false
      },
      "k1": {
        "S": "cea899cd-edd5-44f0-aca8-f2ab0e52f5b9"
      }
    },
    "SizeBytes": 745
  },
  "eventID": "3bd669d8-0ff0-4af2-9bfb-8e32077068fc",
  "eventName": "MODIFY",
  "userIdentity": null,
  "recordFormat": "application/json",
  "tableName": "<ddb-table-name>",
  "eventSource": "aws:dynamodb"
}
*/

const { unmarshall } = require("@aws-sdk/util-dynamodb");
const jsonPP = (json) => console.log(JSON.stringify(json, undefined, 2));

const processRecord = (record) => {
  try {
    const ddbStreamData = JSON.parse(Buffer.from(record.data, 'base64').toString());

    const result = {
      recordId: record.recordId,
      approximateArrivalTimestamp: record.approximateArrivalTimestamp,
      eventID: ddbStreamData.eventID,
      eventName: ddbStreamData.eventName,
      tableName: ddbStreamData.tableName,
      ApproximateCreationDateTime: ddbStreamData.dynamodb.ApproximateCreationDateTime,
      SizeBytes: ddbStreamData.dynamodb.SizeBytes,
    };

    if (ddbStreamData.dynamodb.NewImage) {
      result.NewImage = unmarshall(ddbStreamData.dynamodb.NewImage);
    }
    if (ddbStreamData.dynamodb.OldImage) {
      result.OldImage = unmarshall(ddbStreamData.dynamodb.OldImage);
    }

    // Add newline and encode base64
    const encodedResult = Buffer.from(`${JSON.stringify(result)}\n`).toString('base64');

    return {
      recordId: record.recordId,
      result: 'Ok',
      data: encodedResult
    }
  } catch (error) {
    return {
      recordId: record.recordId,
      result: 'ProcessingFailed',
      data: record.data
    }
  }
}

const main = async (event, context) => {
  const transformedData = event.records.map(processRecord);

  const okData = transformedData.filter(d => d.result === 'Ok');
  if (okData.length > 0) console.log(okData[0]);
  const failedData = transformedData.filter(d => d.result === 'ProcessingFailed')
  if (failedData.length > 0) console.log(failedData[0]);
  console.log(`Total:${transformedData.length}, OK:${okData.length}, NG:${failedData.length}`);

  return { records: transformedData };
}

exports.handler = async (event, context) => main(event, context)

// local execution
if (!process.env.AWS_LAMBDA_FUNCTION_VERSION) {
  const event = { 'plain_text': 'this is local exec' };
  const context = { 'test': 'test' };
  this.handler(event, context);
}
