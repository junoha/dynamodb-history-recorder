"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.handler = void 0;
const util_dynamodb_1 = require("@aws-sdk/util-dynamodb");
/**
 * Lambda handler
 * @param event event
 */
const handler = async (event) => {
    const transformedData = event.records.map(processRecord);
    const okData = transformedData.filter(d => d.result === 'Ok');
    if (okData.length > 0)
        console.log(okData[0]);
    const failedData = transformedData.filter(d => d.result === 'ProcessingFailed');
    if (failedData.length > 0)
        console.log(failedData[0]);
    console.log(`Total:${transformedData.length}, OK:${okData.length}, NG:${failedData.length}`);
    return { records: transformedData };
};
exports.handler = handler;
/**
 * Firehose event record
 * @param record
 * @returns
 */
function processRecord(record) {
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
            result.NewImage = (0, util_dynamodb_1.unmarshall)(ddbStreamData.dynamodb.NewImage);
        }
        if (ddbStreamData.dynamodb.OldImage) {
            result.OldImage = (0, util_dynamodb_1.unmarshall)(ddbStreamData.dynamodb.OldImage);
        }
        // Add newline and encode base64
        const encodedResult = Buffer.from(`${JSON.stringify(result)}\n`).toString('base64');
        return {
            recordId: record.recordId,
            result: 'Ok',
            data: encodedResult
        };
    }
    catch (error) {
        return {
            recordId: record.recordId,
            result: 'ProcessingFailed',
            data: record.data
        };
    }
}
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaW5kZXguanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyJpbmRleC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7QUFNQSwwREFBb0Q7QUFFcEQ7OztHQUdHO0FBQ0ksTUFBTSxPQUFPLEdBQWtDLEtBQUssRUFBRSxLQUFrQyxFQUFFLEVBQUU7SUFDakcsTUFBTSxlQUFlLEdBQUcsS0FBSyxDQUFDLE9BQU8sQ0FBQyxHQUFHLENBQUMsYUFBYSxDQUFDLENBQUM7SUFFekQsTUFBTSxNQUFNLEdBQUcsZUFBZSxDQUFDLE1BQU0sQ0FBQyxDQUFDLENBQUMsRUFBRSxDQUFDLENBQUMsQ0FBQyxNQUFNLEtBQUssSUFBSSxDQUFDLENBQUM7SUFDOUQsSUFBSSxNQUFNLENBQUMsTUFBTSxHQUFHLENBQUM7UUFBRSxPQUFPLENBQUMsR0FBRyxDQUFDLE1BQU0sQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDO0lBQzlDLE1BQU0sVUFBVSxHQUFHLGVBQWUsQ0FBQyxNQUFNLENBQUMsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxDQUFDLENBQUMsTUFBTSxLQUFLLGtCQUFrQixDQUFDLENBQUE7SUFDL0UsSUFBSSxVQUFVLENBQUMsTUFBTSxHQUFHLENBQUM7UUFBRSxPQUFPLENBQUMsR0FBRyxDQUFDLFVBQVUsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDO0lBQ3RELE9BQU8sQ0FBQyxHQUFHLENBQUMsU0FBUyxlQUFlLENBQUMsTUFBTSxRQUFRLE1BQU0sQ0FBQyxNQUFNLFFBQVEsVUFBVSxDQUFDLE1BQU0sRUFBRSxDQUFDLENBQUM7SUFFN0YsT0FBTyxFQUFFLE9BQU8sRUFBRSxlQUFlLEVBQUUsQ0FBQztBQUN0QyxDQUFDLENBQUE7QUFWWSxRQUFBLE9BQU8sV0FVbkI7QUFjRDs7OztHQUlHO0FBQ0gsU0FBUyxhQUFhLENBQUMsTUFBeUM7SUFDOUQsSUFBSTtRQUNGLE1BQU0sYUFBYSxHQUFHLElBQUksQ0FBQyxLQUFLLENBQUMsTUFBTSxDQUFDLElBQUksQ0FBQyxNQUFNLENBQUMsSUFBSSxFQUFFLFFBQVEsQ0FBQyxDQUFDLFFBQVEsRUFBRSxDQUFDLENBQUM7UUFFaEYsTUFBTSxNQUFNLEdBQWU7WUFDekIsUUFBUSxFQUFFLE1BQU0sQ0FBQyxRQUFRO1lBQ3pCLDJCQUEyQixFQUFFLE1BQU0sQ0FBQywyQkFBMkI7WUFDL0QsT0FBTyxFQUFFLGFBQWEsQ0FBQyxPQUFPO1lBQzlCLFNBQVMsRUFBRSxhQUFhLENBQUMsU0FBUztZQUNsQyxTQUFTLEVBQUUsYUFBYSxDQUFDLFNBQVM7WUFDbEMsMkJBQTJCLEVBQUUsYUFBYSxDQUFDLFFBQVEsQ0FBQywyQkFBMkI7WUFDL0UsU0FBUyxFQUFFLGFBQWEsQ0FBQyxRQUFRLENBQUMsU0FBUztTQUM1QyxDQUFDO1FBRUYsSUFBSSxhQUFhLENBQUMsUUFBUSxDQUFDLFFBQVEsRUFBRTtZQUNuQyxNQUFNLENBQUMsUUFBUSxHQUFHLElBQUEsMEJBQVUsRUFBQyxhQUFhLENBQUMsUUFBUSxDQUFDLFFBQVEsQ0FBQyxDQUFDO1NBQy9EO1FBQ0QsSUFBSSxhQUFhLENBQUMsUUFBUSxDQUFDLFFBQVEsRUFBRTtZQUNuQyxNQUFNLENBQUMsUUFBUSxHQUFHLElBQUEsMEJBQVUsRUFBQyxhQUFhLENBQUMsUUFBUSxDQUFDLFFBQVEsQ0FBQyxDQUFDO1NBQy9EO1FBRUQsZ0NBQWdDO1FBQ2hDLE1BQU0sYUFBYSxHQUFHLE1BQU0sQ0FBQyxJQUFJLENBQUMsR0FBRyxJQUFJLENBQUMsU0FBUyxDQUFDLE1BQU0sQ0FBQyxJQUFJLENBQUMsQ0FBQyxRQUFRLENBQUMsUUFBUSxDQUFDLENBQUM7UUFFcEYsT0FBTztZQUNMLFFBQVEsRUFBRSxNQUFNLENBQUMsUUFBUTtZQUN6QixNQUFNLEVBQUUsSUFBSTtZQUNaLElBQUksRUFBRSxhQUFhO1NBQ3BCLENBQUE7S0FDRjtJQUFDLE9BQU8sS0FBSyxFQUFFO1FBQ2QsT0FBTztZQUNMLFFBQVEsRUFBRSxNQUFNLENBQUMsUUFBUTtZQUN6QixNQUFNLEVBQUUsa0JBQWtCO1lBQzFCLElBQUksRUFBRSxNQUFNLENBQUMsSUFBSTtTQUNsQixDQUFBO0tBQ0Y7QUFDSCxDQUFDO0FBRUQ7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7RUFnRUUiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQge1xuICBGaXJlaG9zZVRyYW5zZm9ybWF0aW9uRXZlbnQsXG4gIEZpcmVob3NlVHJhbnNmb3JtYXRpb25FdmVudFJlY29yZCxcbiAgRmlyZWhvc2VUcmFuc2Zvcm1hdGlvbkhhbmRsZXIsXG4gIEZpcmVob3NlVHJhbnNmb3JtYXRpb25SZXN1bHRSZWNvcmQsXG59IGZyb20gXCJhd3MtbGFtYmRhXCI7XG5pbXBvcnQgeyB1bm1hcnNoYWxsIH0gZnJvbSBcIkBhd3Mtc2RrL3V0aWwtZHluYW1vZGJcIjtcblxuLyoqXG4gKiBMYW1iZGEgaGFuZGxlclxuICogQHBhcmFtIGV2ZW50IGV2ZW50XG4gKi9cbmV4cG9ydCBjb25zdCBoYW5kbGVyOiBGaXJlaG9zZVRyYW5zZm9ybWF0aW9uSGFuZGxlciA9IGFzeW5jIChldmVudDogRmlyZWhvc2VUcmFuc2Zvcm1hdGlvbkV2ZW50KSA9PiB7XG4gIGNvbnN0IHRyYW5zZm9ybWVkRGF0YSA9IGV2ZW50LnJlY29yZHMubWFwKHByb2Nlc3NSZWNvcmQpO1xuXG4gIGNvbnN0IG9rRGF0YSA9IHRyYW5zZm9ybWVkRGF0YS5maWx0ZXIoZCA9PiBkLnJlc3VsdCA9PT0gJ09rJyk7XG4gIGlmIChva0RhdGEubGVuZ3RoID4gMCkgY29uc29sZS5sb2cob2tEYXRhWzBdKTtcbiAgY29uc3QgZmFpbGVkRGF0YSA9IHRyYW5zZm9ybWVkRGF0YS5maWx0ZXIoZCA9PiBkLnJlc3VsdCA9PT0gJ1Byb2Nlc3NpbmdGYWlsZWQnKVxuICBpZiAoZmFpbGVkRGF0YS5sZW5ndGggPiAwKSBjb25zb2xlLmxvZyhmYWlsZWREYXRhWzBdKTtcbiAgY29uc29sZS5sb2coYFRvdGFsOiR7dHJhbnNmb3JtZWREYXRhLmxlbmd0aH0sIE9LOiR7b2tEYXRhLmxlbmd0aH0sIE5HOiR7ZmFpbGVkRGF0YS5sZW5ndGh9YCk7XG5cbiAgcmV0dXJuIHsgcmVjb3JkczogdHJhbnNmb3JtZWREYXRhIH07XG59XG5cbnR5cGUgZGRiSGlzdG9yeSA9IHtcbiAgcmVjb3JkSWQ6IHN0cmluZyxcbiAgYXBwcm94aW1hdGVBcnJpdmFsVGltZXN0YW1wOiBudW1iZXIsXG4gIGV2ZW50SUQ6IHN0cmluZyxcbiAgZXZlbnROYW1lOiBzdHJpbmcsXG4gIHRhYmxlTmFtZTogc3RyaW5nLFxuICBBcHByb3hpbWF0ZUNyZWF0aW9uRGF0ZVRpbWU6IG51bWJlcixcbiAgU2l6ZUJ5dGVzOiBudW1iZXIsXG4gIE5ld0ltYWdlPzogYW55LFxuICBPbGRJbWFnZT86IGFueSxcbn1cblxuLyoqXG4gKiBGaXJlaG9zZSBldmVudCByZWNvcmRcbiAqIEBwYXJhbSByZWNvcmQgXG4gKiBAcmV0dXJucyBcbiAqL1xuZnVuY3Rpb24gcHJvY2Vzc1JlY29yZChyZWNvcmQ6IEZpcmVob3NlVHJhbnNmb3JtYXRpb25FdmVudFJlY29yZCk6IEZpcmVob3NlVHJhbnNmb3JtYXRpb25SZXN1bHRSZWNvcmQge1xuICB0cnkge1xuICAgIGNvbnN0IGRkYlN0cmVhbURhdGEgPSBKU09OLnBhcnNlKEJ1ZmZlci5mcm9tKHJlY29yZC5kYXRhLCAnYmFzZTY0JykudG9TdHJpbmcoKSk7XG5cbiAgICBjb25zdCByZXN1bHQ6IGRkYkhpc3RvcnkgPSB7XG4gICAgICByZWNvcmRJZDogcmVjb3JkLnJlY29yZElkLFxuICAgICAgYXBwcm94aW1hdGVBcnJpdmFsVGltZXN0YW1wOiByZWNvcmQuYXBwcm94aW1hdGVBcnJpdmFsVGltZXN0YW1wLFxuICAgICAgZXZlbnRJRDogZGRiU3RyZWFtRGF0YS5ldmVudElELFxuICAgICAgZXZlbnROYW1lOiBkZGJTdHJlYW1EYXRhLmV2ZW50TmFtZSxcbiAgICAgIHRhYmxlTmFtZTogZGRiU3RyZWFtRGF0YS50YWJsZU5hbWUsXG4gICAgICBBcHByb3hpbWF0ZUNyZWF0aW9uRGF0ZVRpbWU6IGRkYlN0cmVhbURhdGEuZHluYW1vZGIuQXBwcm94aW1hdGVDcmVhdGlvbkRhdGVUaW1lLFxuICAgICAgU2l6ZUJ5dGVzOiBkZGJTdHJlYW1EYXRhLmR5bmFtb2RiLlNpemVCeXRlcyxcbiAgICB9O1xuXG4gICAgaWYgKGRkYlN0cmVhbURhdGEuZHluYW1vZGIuTmV3SW1hZ2UpIHtcbiAgICAgIHJlc3VsdC5OZXdJbWFnZSA9IHVubWFyc2hhbGwoZGRiU3RyZWFtRGF0YS5keW5hbW9kYi5OZXdJbWFnZSk7XG4gICAgfVxuICAgIGlmIChkZGJTdHJlYW1EYXRhLmR5bmFtb2RiLk9sZEltYWdlKSB7XG4gICAgICByZXN1bHQuT2xkSW1hZ2UgPSB1bm1hcnNoYWxsKGRkYlN0cmVhbURhdGEuZHluYW1vZGIuT2xkSW1hZ2UpO1xuICAgIH1cblxuICAgIC8vIEFkZCBuZXdsaW5lIGFuZCBlbmNvZGUgYmFzZTY0XG4gICAgY29uc3QgZW5jb2RlZFJlc3VsdCA9IEJ1ZmZlci5mcm9tKGAke0pTT04uc3RyaW5naWZ5KHJlc3VsdCl9XFxuYCkudG9TdHJpbmcoJ2Jhc2U2NCcpO1xuXG4gICAgcmV0dXJuIHtcbiAgICAgIHJlY29yZElkOiByZWNvcmQucmVjb3JkSWQsXG4gICAgICByZXN1bHQ6ICdPaycsXG4gICAgICBkYXRhOiBlbmNvZGVkUmVzdWx0XG4gICAgfVxuICB9IGNhdGNoIChlcnJvcikge1xuICAgIHJldHVybiB7XG4gICAgICByZWNvcmRJZDogcmVjb3JkLnJlY29yZElkLFxuICAgICAgcmVzdWx0OiAnUHJvY2Vzc2luZ0ZhaWxlZCcsXG4gICAgICBkYXRhOiByZWNvcmQuZGF0YVxuICAgIH1cbiAgfVxufVxuXG4vKlxuU3RyZWFtIGV2ZW50IGZvcm1hdFxue1xuICBcImludm9jYXRpb25JZFwiOiBcIjUxMGE4ODFmLTg5NDAtNDkzMy05YjFmLTJjMjY5MjQwYTg3YVwiLFxuICBcInNvdXJjZUtpbmVzaXNTdHJlYW1Bcm5cIjogXCJhcm46YXdzOmtpbmVzaXM6PHJlZ2lvbi1uYW1lPjo8YWNjb3VudC1pZD46c3RyZWFtLzxkZGItdGFibGUtbmFtZT5cIixcbiAgXCJkZWxpdmVyeVN0cmVhbUFyblwiOiBcImFybjphd3M6ZmlyZWhvc2U6PHJlZ2lvbi1uYW1lPjo8YWNjb3VudC1pZD46ZGVsaXZlcnlzdHJlYW0vPGRkYi10YWJsZS1uYW1lPlwiLFxuICBcInJlZ2lvblwiOiBcIjxyZWdpb24tbmFtZT5cIixcbiAgXCJyZWNvcmRzXCI6IFtcbiAgICB7XG4gICAgICBcInJlY29yZElkXCI6IFwiNDk2MTUzMzIxNzU1MDQzOTkyNzUzODg5OTk1MTk3NjQyNTU1NTk3NzcyNjQwNzE5MTY2NTA0OTgwMDAwMDBcIixcbiAgICAgIFwiYXBwcm94aW1hdGVBcnJpdmFsVGltZXN0YW1wXCI6IDE2MTMwMjI2MDMwMTcsXG4gICAgICBcImRhdGFcIjogXCIuLi5iYXNlNjQuLi5cIixcbiAgICAgIFwia2luZXNpc1JlY29yZE1ldGFkYXRhXCI6IHtcbiAgICAgICAgXCJzZXF1ZW5jZU51bWJlclwiOiBcIjQ5NjE1MzMyMTc1NTA0Mzk5Mjc1Mzg4OTk5NTE5NzY0MjU1NTU5Nzc3MjY0MDcxOTE2NjUwNDk4XCIsXG4gICAgICAgIFwic3Vic2VxdWVuY2VOdW1iZXJcIjogMCxcbiAgICAgICAgXCJwYXJ0aXRpb25LZXlcIjogXCIwQzM3QzNFMzk3NjFCOEYzNzZGMDE5QjI1NzJCODA4MVwiLFxuICAgICAgICBcInNoYXJkSWRcIjogXCJzaGFyZElkLTAwMDAwMDAwMDAwMFwiLFxuICAgICAgICBcImFwcHJveGltYXRlQXJyaXZhbFRpbWVzdGFtcFwiOiAxNjEzMDIyNjAzMDE3XG4gICAgICB9XG4gICAgfVxuICBdXG59XG5cbmJhc2U2NCBkZWNvZGVkIHJlY29yZHNbXS5kYXRhXG57XG4gIFwiYXdzUmVnaW9uXCI6IFwiPHJlZ2lvbi1uYW1lPlwiLFxuICBcImR5bmFtb2RiXCI6IHtcbiAgICBcIkFwcHJveGltYXRlQ3JlYXRpb25EYXRlVGltZVwiOiAxNjEzMDIyNjQ0NjQzLFxuICAgIFwiS2V5c1wiOiB7XG4gICAgICBcImsxXCI6IHtcbiAgICAgICAgXCJTXCI6IFwiY2VhODk5Y2QtZWRkNS00NGYwLWFjYTgtZjJhYjBlNTJmNWI5XCJcbiAgICAgIH1cbiAgICB9LFxuICAgIFwiTmV3SW1hZ2VcIjoge1xuICAgICAgXCJwcm9kdWN0XCI6IHtcbiAgICAgICAgXCJTXCI6IFwiR2xvdmVzIGFhYWFcIlxuICAgICAgfSxcbiAgICAgIFwiYWN0aXZlXCI6IHtcbiAgICAgICAgXCJCT09MXCI6IGZhbHNlXG4gICAgICB9LFxuICAgICAgXCJrMVwiOiB7XG4gICAgICAgIFwiU1wiOiBcImNlYTg5OWNkLWVkZDUtNDRmMC1hY2E4LWYyYWIwZTUyZjViOVwiXG4gICAgICB9XG4gICAgfSxcbiAgICBcIk9sZEltYWdlXCI6IHtcbiAgICAgIFwicHJvZHVjdFwiOiB7XG4gICAgICAgIFwiU1wiOiBcIkdsb3Zlc1wiXG4gICAgICB9LFxuICAgICAgXCJhY3RpdmVcIjoge1xuICAgICAgICBcIkJPT0xcIjogZmFsc2VcbiAgICAgIH0sXG4gICAgICBcImsxXCI6IHtcbiAgICAgICAgXCJTXCI6IFwiY2VhODk5Y2QtZWRkNS00NGYwLWFjYTgtZjJhYjBlNTJmNWI5XCJcbiAgICAgIH1cbiAgICB9LFxuICAgIFwiU2l6ZUJ5dGVzXCI6IDc0NVxuICB9LFxuICBcImV2ZW50SURcIjogXCIzYmQ2NjlkOC0wZmYwLTRhZjItOWJmYi04ZTMyMDc3MDY4ZmNcIixcbiAgXCJldmVudE5hbWVcIjogXCJNT0RJRllcIixcbiAgXCJ1c2VySWRlbnRpdHlcIjogbnVsbCxcbiAgXCJyZWNvcmRGb3JtYXRcIjogXCJhcHBsaWNhdGlvbi9qc29uXCIsXG4gIFwidGFibGVOYW1lXCI6IFwiPGRkYi10YWJsZS1uYW1lPlwiLFxuICBcImV2ZW50U291cmNlXCI6IFwiYXdzOmR5bmFtb2RiXCJcbn1cbiovIl19