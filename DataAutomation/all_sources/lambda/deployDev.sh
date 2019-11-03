#!/bin/bash

#Deploy lambda
sam build -t lambdaDev.yaml && sam package --output-template-file lambda_packaged_all.yaml --s3-bucket mids-air-poll-scripts && sam deploy --template-file lambda_packaged_all.yaml --stack-name lambda-air-poll-full --region us-west-2
