import * as cdk from '@aws-cdk/core';
import * as iam from '@aws-cdk/aws-iam';
import * as lambda from '@aws-cdk/aws-lambda';
import * as kinesis from '@aws-cdk/aws-kinesis';
import { CfnDeliveryStream } from '@aws-cdk/aws-kinesisfirehose';
import * as s3 from '@aws-cdk/aws-s3';
import { Duration } from '@aws-cdk/core';
import { PolicyStatement } from '@aws-cdk/aws-iam';

export interface DynamoDbHistoryRecorderStackProps extends cdk.StackProps {
  bucket: string;
  prefix: string;
}

export class DynamoDbHistoryRecorderStack extends cdk.Stack {
  public readonly kinesisStream: kinesis.Stream;
  public readonly transformationFunc: lambda.Function;
  public readonly existingS3Bucket: s3.Bucket;
  public readonly deliveryStream: CfnDeliveryStream;

  constructor(scope: cdk.Construct, id: string, props: DynamoDbHistoryRecorderStackProps) {
    super(scope, id, props);
    this.kinesisStream = this.createKinesisStream(this);
    this.transformationFunc = this.createFunction(this);
    this.existingS3Bucket = this.fromExistingBucket(this, props);
    this.deliveryStream = this.createDeliveryStream(this, props);
  }

  /**
   * Create KDS for DynamoDB
   * @param scope stack scope
   */
  private createKinesisStream(scope: cdk.Construct): kinesis.Stream {
    // Scale up shard count if needed
    return new kinesis.Stream(scope, 'ddb-history-recorder-kds', {
      streamName: 'ddb-history-recorder-kds',
      shardCount: 1,
      retentionPeriod: Duration.hours(24)
    });
  }

  /**
   * Create Lambda function for KDH data transformation
   * @param scope stack scope
   */
  private createFunction(scope: cdk.Construct): lambda.Function {
    const lambdaRole = new iam.Role(scope, 'ddb-history-recorder-lambda-role', {
      roleName: 'ddb-history-recorder-lambda-role',
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole')
      ],
    });
    return new lambda.Function(this, 'ddb-history-recorder-function', {
      functionName: 'ddb-history-recorder-function',
      code: lambda.Code.fromAsset(`${__dirname}/lambda`),
      handler: 'index.handler',
      runtime: lambda.Runtime.NODEJS_14_X,
      timeout: Duration.seconds(180),
      role: lambdaRole
    });
  }

  /**
   * Get existing S3 bucket
   * @param scope stack scope
   * @param props stack props
   */
  private fromExistingBucket(scope: cdk.Construct, props: DynamoDbHistoryRecorderStackProps): s3.Bucket {
    return s3.Bucket.fromBucketName(scope, 'ddb-history-recorder-bucket', props.bucket) as s3.Bucket;
  }

  /**
   * Create KDH
   * @param scope stack scope
   * @param props stack props
   */
  private createDeliveryStream(scope: cdk.Construct, props: DynamoDbHistoryRecorderStackProps): CfnDeliveryStream {
    const firehoseRole = this.createFirehoseRole(scope);

    // Check buffering config if needed
    return new CfnDeliveryStream(scope, 'ddb-history-recorder-kdh', {
      deliveryStreamName: 'ddb-history-recorder-kdh',
      deliveryStreamType: 'KinesisStreamAsSource',
      kinesisStreamSourceConfiguration: {
        kinesisStreamArn: this.kinesisStream.streamArn,
        roleArn: firehoseRole.roleArn
      },
      extendedS3DestinationConfiguration: {
        bucketArn: this.existingS3Bucket.bucketArn,
        prefix: `${props.prefix}/dest/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/`,
        errorOutputPrefix: `${props.prefix}/error/`,
        roleArn: firehoseRole.roleArn,
        bufferingHints: {
          intervalInSeconds: 180,
          sizeInMBs: 3
        },
        compressionFormat: 'GZIP',
        cloudWatchLoggingOptions: {
          enabled: true,
          logGroupName: `/aws/kinesisfirehose/ddb-history-recorder-kdh`,
          logStreamName: 'S3Delivery'
        },
        s3BackupMode: 'Disabled',
        processingConfiguration: {
          enabled: true,
          processors: [
            {
              type: 'Lambda',
              parameters: [
                { parameterName: 'BufferIntervalInSeconds', parameterValue: '180' },
                { parameterName: 'BufferSizeInMBs', parameterValue: '3' },
                { parameterName: 'LambdaArn', parameterValue: this.transformationFunc.functionArn },
                { parameterName: 'NumberOfRetries', parameterValue: '3' },
                { parameterName: 'RoleArn', parameterValue: firehoseRole.roleArn }
              ]
            }
          ]
        },
      }
    });
  }

  /**
   * Create IAM Role for KDH delivery stream
   * @param scope stack stope
   */
  private createFirehoseRole(scope: cdk.Construct): iam.Role {
    return new iam.Role(scope, 'ddb-history-recorder-kdh-role', {
      roleName: 'ddb-history-recorder-kdh-role',
      assumedBy: new iam.ServicePrincipal('firehose.amazonaws.com'),
      inlinePolicies: {
        'ddb-history-recorder-kdh-role-policy': new iam.PolicyDocument({
          statements: [
            new PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: [
                'kinesis:DescribeStream',
                'kinesis:GetShardIterator',
                'kinesis:GetRecords',
                'kinesis:ListShard',
              ],
              resources: [
                this.kinesisStream.streamArn
              ]
            }),
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: [
                's3:AbortMultipartUpload',
                's3:GetBucketLocation',
                's3:GetObject',
                's3:ListBucket',
                's3:ListBucketMultipartUploads',
                's3:PutObject'
              ],
              resources: [
                this.existingS3Bucket.bucketArn,
                `${this.existingS3Bucket.bucketArn}/*`
              ],
            }),
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: [
                'lambda:InvokeFunction',
                'lambda:GetFunctionConfiguration',
              ],
              resources: [
                this.transformationFunc.functionArn
              ]
            }),
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: [
                'glue:GetTable',
                'glue:GetTableVersion',
                'glue:GetTableVersions',
                'logs:PutLogEvents'
              ],
              resources: ['*']
            })
          ]
        })
      }
    });
  }

}
