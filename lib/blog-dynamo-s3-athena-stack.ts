import { Stack, StackProps, RemovalPolicy, Duration } from "aws-cdk-lib";
import { Match, Rule } from "aws-cdk-lib/aws-events";
import { NodejsFunction } from "aws-cdk-lib/aws-lambda-nodejs";
import { Bucket, BlockPublicAccess, ObjectOwnership } from "aws-cdk-lib/aws-s3";
import { Construct } from "constructs";
import { LambdaFunction } from "aws-cdk-lib/aws-events-targets";
import { Architecture, Runtime } from "aws-cdk-lib/aws-lambda";
import { RetentionDays } from "aws-cdk-lib/aws-logs";
import {
  ManagedPolicy,
  Policy,
  PolicyStatement,
  Role,
  ServicePrincipal,
} from "aws-cdk-lib/aws-iam";
import { CfnWorkGroup } from "aws-cdk-lib/aws-athena";
import { CfnCrawler, CfnDatabase } from "aws-cdk-lib/aws-glue";

export interface BlogDynamoS3AthenaStackProps extends StackProps {
  cdcSchemaPaths: string[];
}

export class BlogDynamoS3AthenaStack extends Stack {
  constructor(scope: Construct, id: string, props: BlogDynamoS3AthenaStackProps) {
    super(scope, id, props);

    const { cdcSchemaPaths } = props;

    const rawDataBucket = new Bucket(this, "AthenaRawDataBucket", {
      removalPolicy: RemovalPolicy.DESTROY,
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      autoDeleteObjects: true,
      objectOwnership: ObjectOwnership.BUCKET_OWNER_ENFORCED,
    });

    const athenaArchiveFn = new NodejsFunction(this, "athenaArchiveFn", {
      entry: "./lib/athena-archive.ts",
      runtime: Runtime.NODEJS_20_X,
      memorySize: 1024,
      timeout: Duration.seconds(30),
      logRetention: RetentionDays.ONE_DAY,
      retryAttempts: 0,
      architecture: Architecture.ARM_64,
      environment: {
        CDC_ARCHIVE_BUCKET: rawDataBucket.bucketName,
      },
    });

    rawDataBucket.grantReadWrite(athenaArchiveFn);

    const sources = cdcSchemaPaths.reduce((p, c) => {
      const [basePath] = c.split("/");
      return p.includes(basePath) ? p : [...p, basePath];
    }, [] as string[]);
    new Rule(this, "AthenaArchiveRule", {
      eventPattern: {
        source: sources,
        detailType: ["dynamo.item.changed"],
        detail: {
          data: {
            operation: Match.exists(),
          },
        },
      },
      targets: [new LambdaFunction(athenaArchiveFn)],
    });

    const glueRole = new Role(this, "AthenaGlueRole", {
      assumedBy: new ServicePrincipal("glue.amazonaws.com"),
      managedPolicies: [
        ManagedPolicy.fromAwsManagedPolicyName(
          "service-role/AWSGlueServiceRole"
        ),
        ManagedPolicy.fromAwsManagedPolicyName("AmazonS3FullAccess"),
      ],
    });

    rawDataBucket.grantReadWrite(glueRole);

    const glueDatabaseName = "martzcodes";
    new CfnCrawler(this, "Crawler", {
      role: glueRole.roleArn,
      databaseName: glueDatabaseName,
      configuration: JSON.stringify({
        Version: 1.0,
        Grouping: {
          TableGroupingPolicy: "CombineCompatibleSchemas",
        },
        CrawlerOutput: {
          Tables: { AddOrUpdateBehavior: "MergeNewColumns" },
        },
      }),
      targets: {
        s3Targets: cdcSchemaPaths.map((path) => ({ path: `s3://${rawDataBucket.bucketName}/${path}` })),
      },
    });

    const athenaResultsBucket = new Bucket(this, "AthenaResultsBucket", {
      removalPolicy: RemovalPolicy.DESTROY,
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      autoDeleteObjects: true,
      objectOwnership: ObjectOwnership.BUCKET_OWNER_ENFORCED,
    });

    const athenaWorkgroupName = "martzcodes";
    new CfnWorkGroup(this, "AthenaWorkGroup", {
      name: athenaWorkgroupName,
      state: "ENABLED",
      recursiveDeleteOption: true,
      description: "Workgroup for POC",
      workGroupConfiguration: {
        enforceWorkGroupConfiguration: true,
        resultConfiguration: {
          outputLocation: `s3://${athenaResultsBucket.bucketName}/`,
        },
      },
    });

    // Create a Glue database
    new CfnDatabase(this, `${id}-GlueDatabase`, {
      catalogId: this.account,
      databaseInput: {
        name: glueDatabaseName,
        locationUri: `s3://${athenaResultsBucket.bucketName}/`,
        description: "Glue database to be used in simple s3-athena workflows",
      },
    });

    // Create a policy that grants full access to Athena and Glue
    const athenaPolicy = new Policy(this, `${id}-AthenaGluePolicy`, {
      statements: [
        new PolicyStatement({
          actions: ["athena:*", "glue:*"],
          resources: ["*"],
        }),
      ],
    });

    // Create IAM role that will grant access to Glue, Athena and the S3
    const glueAthenaS3Role = new Role(this, `${id}-GlueS3Role`, {
      roleName: `glue-athena-s3-role`,
      assumedBy: new ServicePrincipal("glue.amazonaws.com"),
      description: "Role for Glue-Athena based S3 workflows",
    });

    // Grant read/write access to the S3 buckets for the glueAthenaS3Role
    rawDataBucket.grantReadWrite(glueAthenaS3Role);
    athenaResultsBucket.grantReadWrite(glueAthenaS3Role);

    // Attach the athena policy to the glueAthenaS3Role
    glueAthenaS3Role.attachInlinePolicy(athenaPolicy);
  }
}
