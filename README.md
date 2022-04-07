# IPFS Elastic Provider - Bucket to Indexer

The purpose of this component is to notify indexer subsystem of the existence of a new object in a S3 bucket. It receives a PUT notification trigger, transform it into a message with the expected format and send it to the indexer SQS queue.

This lambda can be triggered by any S3 buckets within the same region it was deployed. It also can be deployed in any region.

## Deployment environment variables

_Variables in bold are required._

| Name                        | Default            | Description                                                                    |
| --------------------------- | ------------------ | ------------------------------------------------------------------------------ |
| SQS_INDEXER_QUEUE_URL       | indexerQueue       | The SQS topic to publish message to indexing subsystem                         |
