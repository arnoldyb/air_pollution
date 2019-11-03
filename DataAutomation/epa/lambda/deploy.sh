#!/bin/bash

#Deploy lambda
sam build -t lambdaDev.yaml && sam package --output-template-file lambda_packaged_epa.yaml --s3-bucket mids-air-poll-scripts && sam deploy --template-file lambda_packaged_epa.yaml --stack-name lambda-epa --region us-west-2
