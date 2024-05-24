/**
 *  Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance
 *  with the License. A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  or in the 'license' file accompanying this file. This file is distributed on an 'AS IS' BASIS, WITHOUT WARRANTIES
 *  OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions
 *  and limitations under the License.
 */

import { GetObjectCommandInput, PutObjectCommandInput, PutObjectCommandOutput, S3 as AWS_S3 } from '@aws-sdk/client-s3';
import { AwsCredentialIdentity } from '@smithy/types';
import { throttlingBackOff } from './backoff';

export class S3 {
  private readonly client: AWS_S3;

  public constructor(credentials?: AwsCredentialIdentity) {
    this.client = new AWS_S3({ credentials, logger: console });
  }

  async getObjectBody(input: GetObjectCommandInput): Promise<string> {
    const object = await throttlingBackOff(() => this.client.getObject(input));
    return object.Body?.transformToString()!;
  }

  async getObjectBodyAsString(input: GetObjectCommandInput): Promise<string> {
    return this.getObjectBody(input).then(body => body.toString());
  }

  async putObject(input: PutObjectCommandInput): Promise<PutObjectCommandOutput> {
    return throttlingBackOff(() => this.client.putObject(input));
  }
}
