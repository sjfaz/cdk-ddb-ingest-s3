# CDK Ingest CSV from S3 to DynamoDB

Event notification on S3 will trigger when a new CSV object is added.

Lambda will process the CSV and add to DynamoDB using BatchWrite Item.

The lambda is set up to import csv from this dataset:
https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads

Example data is in this repo at example-data.csv.

NB. The best way now to do csv Import from S3 is to use the feature built in to DynamoDB. This will be most cost effective.

## Useful commands

- `npm run build` compile typescript to js
- `npm run watch` watch for changes and compile
- `npm run test` perform the jest unit tests
- `cdk deploy` deploy this stack to your default AWS account/region
- `cdk diff` compare deployed stack with current state
- `cdk synth` emits the synthesized CloudFormation template
