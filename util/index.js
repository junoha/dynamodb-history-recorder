const { DynamoDBClient, PutItemCommand } = require('@aws-sdk/client-dynamodb');
const { marshall, unmarshall } = require("@aws-sdk/util-dynamodb");
const faker = require('faker');

let template = `
{
  "k1": "{{random.uuid}}",
  "country": "{{address.country}}",
  "lat": {{address.latitude}},
  "lon": {{address.longitude}},
  "date":"{{date.past(10)}}",
  "active": {{random.boolean}},
  "person": {
    "firstName": "{{name.firstName}}",
    "lastName": "{{name.lastName}}",
    "age": {{random.number({"min": 1, "max": 100})}}
  },
  "jobs": ["{{name.jobTitle}}","{{name.jobTitle}}","{{name.jobTitle}}"],
  "product": "{{commerce.product}}",
  "productName": "{{commerce.productName}}",
  "amount": {{finance.amount}},
  "nullable": ###nullable###
}
`;

const sleep = msec => new Promise(resolve => setTimeout(resolve, msec));

const ddbClient = new DynamoDBClient({ region: 'ap-northeast-1' });

const putItem = async (item) => {
  try {
    const data = await ddbClient.send(new PutItemCommand(item));
    console.log(data);
  } catch (err) {
    console.error(err);
  }
};

const insert = async () => {
  for (let i = 0; i < process.env.NUM; i++) {
    template = template.replace('###nullable###', faker.helpers.randomize([10, 20, 30, 40, 50, null]));
    const dummy = JSON.parse(faker.fake(template));
    const item = {
      TableName: process.env.TABLE_NAME,
      Item: marshall(dummy),
    };
    await putItem(item);
    await sleep(100);
  }
};

// TABLE_NAME=ddb-table NUM=1000 node index.js > /dev/null
(async () => {
  await insert();
})();
