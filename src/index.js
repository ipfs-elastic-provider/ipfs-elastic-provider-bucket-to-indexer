const { SQSClient, SendMessageCommand } = require('@aws-sdk/client-sqs')
const { logger, serializeError } = require('./logging')
const {
  SQS_INDEXER_QUEUE_URL: indexerQueue,
  SQS_INDEXER_QUEUE_REGION: indexerQueueRegion
} = process.env

const SQSclient = new SQSClient({
  region: indexerQueueRegion
})

/**
 * @type {import('aws-lambda').SNSHandler}
 */
async function main(event) {
  try {
    const records = toS3Event(event).Records
      .filter(r => r.eventName.startsWith('ObjectCreated'))
      .filter(r => r.s3.object.key.endsWith('.car'))
    for (const record of records) {
      const snsMessage = `${record.awsRegion}/${record.s3.bucket.name}/${record.s3.object.key}`
      await publishToSQS(snsMessage)
    }
  } catch (e) {
    logger.error(`${serializeError(e)}`)
    throw e
  }
}

async function publishToSQS(data) {
  const queue = indexerQueue ?? 'indexerQueue'
  try {
    logger.info(`Sending message ${data} to queue ${queue}`)
    const response = await SQSclient.send(
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
 * Extract an S3Event from the passed SNSEvent.
 * @param {import('aws-lambda').SNSEvent} snsEvent
 * @returns {import('aws-lambda').S3Event}
 */
function toS3Event(snsEvent) {
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
      console.error(`failed to extract S3Event record from SNSEvent record: ${err.message}`, snsRec)
    }
  }
  return s3Event
}

exports.handler = main
