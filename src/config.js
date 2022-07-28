'use strict'

const { resolve } = require('path')

/* c8 ignore next */
require('dotenv').config({ path: process.env.ENV_FILE_PATH || resolve(process.cwd(), '.env') })

const {
  SNS_EVENTS_TOPIC: eventsTopic
} = process.env

module.exports = {
  eventsTopic: eventsTopic ?? 'eventsTopic'
}
