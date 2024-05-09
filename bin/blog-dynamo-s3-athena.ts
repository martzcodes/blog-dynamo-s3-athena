#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { BlogDynamoS3AthenaStack } from '../lib/blog-dynamo-s3-athena-stack';
import { CdkDynamodbCdcStack } from '@martzcodes/cdk-dynamodb-cdc/cdk-dynamodb-cdc-stack';

const app = new cdk.App();
const cdcSchemaPaths = [
  "blog/posts",
  "blog/comments",
  "users",
];
new BlogDynamoS3AthenaStack(app, 'BlogDynamoS3AthenaStack', {
  cdcSchemaPaths: cdcSchemaPaths,
});

// Create a stack for each schema
// in this example there are two dynamodb tables with CDC enabled
// the blog table has two item types (schemas) within it
const sources = cdcSchemaPaths.reduce((p, c) => {
  const [basePath] = c.split("/");
  return p.includes(basePath) ? p : [...p, basePath];
}, [] as string[]);

sources.forEach((schema) => {  
  new CdkDynamodbCdcStack(app, `${schema}CdcStack`, {
    cdcLogs: true,
    eventSource: schema,
  });
});