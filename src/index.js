const { logger, serializeError } = require('./logging')
const { SQS_INDEXER_QUEUE_URL: indexerQueue } = process.env

async function main(event) {
  try {
    for (const record of event.Records) {
      const snsMessage = `${record.awsRegion}/${record.s3.bucket.name}/${record.s3.object.key}`
      publishToSQS(snsMessage)
    }
  } catch (e) {
    logger.error(`${serializeError(e)}`)
    throw e
  }
}

async function publishToSQS(data) {
  let queue = indexerQueue ?? 'indexerQueue'
  try {
    logger.log(`Sending message ${data} to queue ${queue}`)
    sqsClient.send(
      new SendMessageCommand({ QueueUrl: queue, MessageBody: data }),
    )
  } catch (e) {
    logger.error(
      `Cannot send message ${data} to ${queue}: ${serializeError(e)}`,
    )
    throw e
  }
}

exports.handler = main
