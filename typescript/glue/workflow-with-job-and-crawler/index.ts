import * as cdk from '@aws-cdk/core';
import * as iam from '@aws-cdk/aws-iam'
import * as glue from '@aws-cdk/aws-glue';

export class GlueStack extends cdk.Stack {
    constructor(scope: cdk.App, id: string, props?: cdk.StackProps) {
        super(scope, id, props);

        const role = new iam.Role(this, 'GlueRole', {
            assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
            managedPolicies: [
                iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole')
            ],
        });

        const sourceDb = new glue.Database(this, 'SourceDatabase', {
            databaseName: 'srcdata'
        });

        new glue.Database(this, 'ParquetDatabase', {
            databaseName: 'parqdata'
        });

        // Using Public Sample Bucket with Avro sample data
        const sourcePath = `s3://crawler-public-${cdk.Stack.of(this).region}/flight/avro/`

        new glue.CfnCrawler(this, 'AvroCrawler', {
            role: role.roleArn,
            targets: {
                s3Targets: [{path: sourcePath}]
            },
            databaseName: sourceDb.databaseName,
        });

    }
}

const app = new cdk.App();

new GlueStack(app, 'MyGlueStack');

app.synth();