import { CfnOutput, Stack, StackProps, RemovalPolicy } from "aws-cdk-lib";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import { NodejsFunction } from "aws-cdk-lib/aws-lambda-nodejs";
import { lambdaFnProps } from "./utils";
import { Construct } from "constructs";

export class CdkDdbIngestS3Stack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const s3Bucket = new s3.Bucket(this, "Bucket", {
      removalPolicy: RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    const table = new dynamodb.Table(this, "S3EventCSVTable", {
      partitionKey: { name: "pk", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "sk", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    const lambdaEventProcessor = new NodejsFunction(this, `s3-event-handler`, {
      ...lambdaFnProps,
      entry: "./services/index.ts",
      handler: "handler",

      environment: {
        TABLE_NAME: table.tableName,
        REGION: process.env.CDK_DEFAULT_REGION!,
      },
    });

    table.grantReadWriteData(lambdaEventProcessor);
    s3Bucket.grantRead(lambdaEventProcessor);

    s3Bucket.addEventNotification(
      s3.EventType.OBJECT_CREATED,
      new s3n.LambdaDestination(lambdaEventProcessor)
      // { prefix: "home/myusername/*" }
    );

    new CfnOutput(this, "bucketName", {
      value: s3Bucket.bucketName,
    });
  }
}
