function createS3URL(bucket, key) {
  return new URL(`s3://${bucket}/${key}`)
}

module.exports = {
  createS3URL
}
