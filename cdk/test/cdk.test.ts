import { expect as expectCDK, matchTemplate, MatchStyle } from '@aws-cdk/assert';
import { App } from 'aws-cdk-lib';
import { DynamoDbHistoryRecorderStack, DynamoDbHistoryRecorderStackProps } from '../lib/ddb-history-recorder';

test('Empty Stack', () => {
  const app = new App();
  const props: DynamoDbHistoryRecorderStackProps = {
    bucket: '',
    prefix: '',
    hivePartition: false,
  };
  // WHEN
  const stack = new DynamoDbHistoryRecorderStack(app, 'MyTestStack', props);
  // THEN
  expectCDK(stack).to(matchTemplate({
    "Resources": {}
  }, MatchStyle.EXACT))
});
