#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from '@aws-cdk/core';
import { DynamoDbHistoryRecorderStack, DynamoDbHistoryRecorderStackProps } from '../lib/ddb-history-recorder';
import { Tags } from '@aws-cdk/core';

function validateAndGetParameter(app: cdk.App) {
  const bucket: string = app.node.tryGetContext('s3-bucket') ?? process.env.S3_BUCKET;
  if (!bucket) {
    throw new Error('s3-bucket must be set. -c s3-bucket=xxx or S3_BUCKET=xxx');
  }

  const prefix: string = app.node.tryGetContext('s3-prefix') ?? process.env.S3_PREFIX;;
  if (!prefix) {
    throw new Error('s3-prefix must be set. -c s3-prefix=xxx or S3_PREFIX=xxx');
  }

  return { bucket, prefix }
}

function main() {
  const app = new cdk.App();
  const param = validateAndGetParameter(app);

  const props: DynamoDbHistoryRecorderStackProps = {
    bucket: param.bucket,
    prefix: param.prefix
  }

  const stack = new DynamoDbHistoryRecorderStack(app, 'DynamoDbHistoryRecorderStack', props);
  Tags.of(stack).add('auto-delete', 'no');
};

main();
