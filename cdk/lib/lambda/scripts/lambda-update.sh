#!/bin/bash

# $ LAMBDA_ARN=<your-lambda-arn> npm run deploy
aws lambda update-function-code --function-name $LAMBDA_ARN --zip-file fileb://`pwd`/build.zip --publish