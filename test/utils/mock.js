'use strict'

const { DynamoDBClient, PutItemCommand, BatchWriteItemCommand } = require('@aws-sdk/client-dynamodb')
const { S3Client } = require('@aws-sdk/client-s3')
const { SendMessageCommand, SQSClient } = require('@aws-sdk/client-sqs')
const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns')
const { mockClient } = require('aws-sdk-client-mock')

const dynamoMock = mockClient(DynamoDBClient)
const s3Mock = mockClient(S3Client)
const sqsMock = mockClient(SQSClient)
const snsMock = mockClient(SNSClient)

function trackDynamoUsages(t) {
  t.context.dynamo = {
    creates: [],
    batchCreates: []
  }

  dynamoMock.on(PutItemCommand).callsFake(params => {
    t.context.dynamo.creates.push(params)
  })

  dynamoMock.on(BatchWriteItemCommand).callsFake(params => {
    t.context.dynamo.batchCreates.push(params)
  })
}

function trackSQSUsages(t) {
  t.context.sqs = {
    publishes: [],
  }

  sqsMock.on(SendMessageCommand).callsFake(params => {
    t.context.sqs.publishes.push(params)
    return {
      MessageId: Math.random().toString().slice(2)
    }
  })
}

function trackSNSUsages(t) {
  t.context.sns = {
    publishes: []
  }

  snsMock.on(PublishCommand).callsFake(params => {
    t.context.sns.publishes.push(params)
  })
}

module.exports = {
  dynamoMock,
  s3Mock,
  snsMock,
  sqsMock,
  trackDynamoUsages,
  trackSQSUsages,
  trackSNSUsages
}
