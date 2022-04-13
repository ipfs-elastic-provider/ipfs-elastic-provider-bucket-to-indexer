const { SQSClient, SendMessageCommand } = require('@aws-sdk/client-sqs')
const { logger, serializeError } = require('./logging')
const {
  SQS_INDEXER_QUEUE_URL: indexerQueue,
  SQS_INDEXER_QUEUE_REGION: indexerQueueRegion
} = process.env

const SQSclient = new SQSClient({
  region: indexerQueueRegion
})

async function main(event) {
  try {
    for (const record of event.Records) {
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

exports.handler = main
