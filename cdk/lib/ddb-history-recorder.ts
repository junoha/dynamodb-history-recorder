import { Construct } from 'constructs';
import {
  Duration,
  Stack,
  StackProps
} from 'aws-cdk-lib';
import {
  Effect,
  ManagedPolicy,
  Role,
  ServicePrincipal,
  PolicyDocument,
  PolicyStatement
} from 'aws-cdk-lib/aws-iam';
import { Stream } from 'aws-cdk-lib/aws-kinesis';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import { Bucket } from 'aws-cdk-lib/aws-s3';
import { CfnDeliveryStream } from 'aws-cdk-lib/aws-kinesisfirehose';


export interface DynamoDbHistoryRecorderStackProps extends StackProps {
  bucket: string;
  prefix: string;
  hivePartition: boolean;
}

export class DynamoDbHistoryRecorderStack extends Stack {
  public readonly kinesisStream: Stream;
  public readonly transformationFunc: NodejsFunction;
  public readonly existingS3Bucket: Bucket;
  public readonly deliveryStream: CfnDeliveryStream;

  constructor(scope: Construct, id: string, props: DynamoDbHistoryRecorderStackProps) {
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
  private createKinesisStream(scope: Construct): Stream {
    // Scale up shard count if needed
    return new Stream(scope, 'ddb-history-recorder-kds', {
      streamName: 'ddb-history-recorder-kds',
      shardCount: 1,
      retentionPeriod: Duration.hours(24)
    });
  }

  /**
   * Create Lambda function for KDH data transformation
   * @param scope stack scope
   */
  private createFunction(scope: Construct): NodejsFunction {
    const lambdaRole = new Role(scope, 'ddb-history-recorder-lambda-role', {
      roleName: 'ddb-history-recorder-lambda-role',
      assumedBy: new ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole')
      ],
    });
    return new NodejsFunction(this, 'ddb-history-recorder-function', {
      functionName: 'ddb-history-recorder-function',
      entry: 'lib/lambda/index.ts',
      role: lambdaRole,
      timeout: Duration.minutes(15),
      bundling: {
        forceDockerBundling: false
      }
    });
  }

  /**
   * Get existing S3 bucket
   * @param scope stack scope
   * @param props stack props
   */
  private fromExistingBucket(scope: Construct, props: DynamoDbHistoryRecorderStackProps): Bucket {
    return Bucket.fromBucketName(scope, 'ddb-history-recorder-bucket', props.bucket) as Bucket;
  }

  /**
   * Create KDH
   * @param scope stack scope
   * @param props stack props
   */
  private createDeliveryStream(scope: Construct, props: DynamoDbHistoryRecorderStackProps): CfnDeliveryStream {
    const firehoseRole = this.createFirehoseRole(scope);
    const prefix = props.hivePartition ? `${props.prefix}/dest/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/` : `${props.prefix}/dest/`;

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
        prefix: prefix,
        errorOutputPrefix: `${props.prefix}/error/`,
        roleArn: firehoseRole.roleArn,
        bufferingHints: {
          intervalInSeconds: 600,
          sizeInMBs: 128
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
                { parameterName: 'BufferIntervalInSeconds', parameterValue: '600' },
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
  private createFirehoseRole(scope: Construct): Role {
    return new Role(scope, 'ddb-history-recorder-kdh-role', {
      roleName: 'ddb-history-recorder-kdh-role',
      assumedBy: new ServicePrincipal('firehose.amazonaws.com'),
      inlinePolicies: {
        'ddb-history-recorder-kdh-role-policy': new PolicyDocument({
          statements: [
            new PolicyStatement({
              effect: Effect.ALLOW,
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
            new PolicyStatement({
              effect: Effect.ALLOW,
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
            new PolicyStatement({
              effect: Effect.ALLOW,
              actions: [
                'lambda:InvokeFunction',
                'lambda:GetFunctionConfiguration',
              ],
              resources: [
                this.transformationFunc.functionArn
              ]
            }),
            new PolicyStatement({
              effect: Effect.ALLOW,
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
