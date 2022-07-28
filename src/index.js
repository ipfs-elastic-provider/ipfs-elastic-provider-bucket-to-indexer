const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns')
const { SQSClient, SendMessageCommand } = require('@aws-sdk/client-sqs')
const { logger: defaultLogger, serializeError } = require('./logging')
const config = require('./config')
const { createS3URL } = require('./s3')
const {
  SQS_INDEXER_QUEUE_URL: indexerQueue,
  SQS_INDEXER_QUEUE_REGION: indexerQueueRegion
} = process.env

const defaultSQSClient = new SQSClient({
  region: indexerQueueRegion
})

function createBucketToIndexerLambda({
  eventsTopic = config.eventsTopic,
  logger = defaultLogger,
  snsClient = new SNSClient(),
  sqsClient = defaultSQSClient
} = {}) {
  /**
   * @type {import('aws-lambda').SNSHandler}
   */
  return async function (event) {
    try {
      const records = toS3Event(event, logger)
        .Records.filter((r) => r.eventName.startsWith('ObjectCreated'))
        .filter((r) => r.s3.object.key.endsWith('.car'))
      for (const record of records) {
        const snsMessage = `${record.awsRegion}/${record.s3.bucket.name}/${record.s3.object.key}`
        await publishToSQS(snsMessage, logger, sqsClient)
        await emitIpfsEvent({
          snsClient,
          eventsTopic,
          event: createIndexerNotifiedEvent({
            s3: record.s3
          })
        })
      }
    } catch (e) {
      logger.error(`${serializeError(e)}`)
      throw e
    }
  }
}

async function publishToSQS(
  data,
  logger = defaultLogger,
  sqsClient = defaultSQSClient
) {
  const queue = indexerQueue ?? 'indexerQueue'
  try {
    logger.info(`Sending message ${data} to queue ${queue}`)
    const response = await sqsClient.send(
      new SendMessageCommand({ QueueUrl: queue, MessageBody: data })
    )
    logger.info(`Success, message sent. MessageID: ${response.MessageId}`)
  } catch (e) {
    logger.error(
      `Cannot send message ${data} to ${queue}: ${serializeError(e)}`
    )
    throw e
  }
}

/**
 * send IndexerNotified event to the appropriate SNS topic
 * @see https://github.com/elastic-ipfs/metrics-collector/blob/main/src/indexer-events/examples/IndexerNotified.json
 * @param {object} arg.s3 - s3 object info
 * @param {string} arg.s3.bucket.name - s3 bucket name
 * @param {string} arg.s3.object.key - s3 key name
 * @param {number} arg.s3.object.size - object size in bytes
 * @param {Date} arg.startTime - time of IndexerNotified event
 */
function createIndexerNotifiedEvent({ s3, startTime = new Date() }) {
  const event = {
    type: 'IndexerNotified',
    uri: createS3URL(s3.bucket.name, s3.object.key).toString(),
    byteLength: s3.object.size,
    startTime: startTime.toISOString()
  }
  return event
}

/**
 * Broadcast an Elastic IPFS Event on the appropriate pubsub topic
 * @param {object} arg.event - elastic-ipfs event to publish
 * @param {string} arg.eventsTopic - topic to publish event on
 */
async function emitIpfsEvent({
  snsClient,
  eventsTopic = config.eventsTopic,
  event
}) {
  await snsClient.send(
    new PublishCommand({
      TopicArn: eventsTopic,
      Message: JSON.stringify(event)
    })
  )
}

/**
 * Extract an S3Event from the passed SNSEvent.
 * @param {import('aws-lambda').SNSEvent} snsEvent
 * @returns {import('aws-lambda').S3Event}
 */
function toS3Event(snsEvent, logger = defaultLogger) {
  const s3Event = { Records: [] }
  for (const snsRec of snsEvent.Records) {
    try {
      for (const s3Rec of JSON.parse(snsRec.Sns.Message).Records || []) {
        if (s3Rec.eventSource !== 'aws:s3') {
          continue
        }
        s3Event.Records.push(s3Rec)
      }
    } catch (err) {
      logger.error(
        `failed to extract S3Event record from SNSEvent record: ${err.message}`,
        snsRec
      )
    }
  }
  return s3Event
}

const handler = createBucketToIndexerLambda()

module.exports = {
  createBucketToIndexerLambda,
  handler
}
