#!/usr/bin/env node
import * as cdk from "aws-cdk-lib";
import { CdkDdbIngestS3Stack } from "../stacks/MainStack";

const app = new cdk.App();
new CdkDdbIngestS3Stack(app, "CdkDdbIngestS3Stack");
