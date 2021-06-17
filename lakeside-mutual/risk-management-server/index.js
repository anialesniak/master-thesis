require('log-timestamp')(() => {
  const timestamp = new Date().toLocaleString('de-CH', {
    timeZone: 'Europe/Zurich',
  })
  return `[${timestamp}] %s`
})
const fs = require('fs')
const nconf = require('nconf')
const path = require('path')
const grpc = require('grpc')
const DataManager = require('./lib/data-manager')
const ReportGenerator = require('./lib/report-generator')

const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'riskmanagement',
  brokers: ['localhost:9092']
})

const consumer = kafka.consumer({ groupId: 'riskmanagement-group' })

nconf
  .argv()
  .env({ lowerCase: true, separator: '_' })
  .file({ file: path.join(__dirname, 'config.json') })

/*
The handleMessage() function gets called whenever a new PolicyEvent message is available
on the message topic. Each message is then persisted with the data manager.
*/
function handleMessage(dataManager, message) {
  const event = JSON.parse(message.value)
  dataManager.addEvent(event)
  dataManager
    .save()
    .catch(error => {
      console.error(`Error: ${error}`)
    })
}

/*
The consumeEvents() function consumes PolicyEvent messages from the topic when
they become available. Each event is then persisted with the data manager.
*/
function consumeEvents(dataManager) {
  const run = async () => {
    await consumer.connect()
    await consumer.subscribe({ topic: 'policy-events', fromBeginning: false })

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        handleMessage(dataManager, message)
      },
    })
  }

  run().catch(console.error)
}

/*
The handleClientRequest() function gets called whenever a request from the Risk Management Client
is received. It then generates a customer data report and sends it back to the client.
 */
function handleClientRequest(dataManager, call, callback) {
  try {
    console.log('Received request from Risk Management Client.')

    let i = 0
    let theInterval = setInterval(() => {
      if (i > 100) {
        const reportGenerator = new ReportGenerator(dataManager.data)
        const csv = reportGenerator.generateCSV()
        const report = { csv }
        call.write({ report })
        call.end()
        console.log('Sent response to Risk Management Client.')
        clearInterval(theInterval)
      } else {
        const progress = i
        call.write({ progress })
      }
      i += 1
    }, 20)
  } catch (error) {
    console.error(`Error: ${error}`)
    callback(null, { csv: '' })
  }
}

/*
The startGRPCServer() function starts the gRPC server which listens for
requests from the Risk Management Client.
 */
function startGRPCServer(dataManager) {
  const grpc_config = nconf.get('grpc')
  const PROTO_PATH = path.join(__dirname, '/riskmanagement.proto')
  const proto = grpc.load(PROTO_PATH).riskmanagement
  const server = new grpc.Server()
  const requestHandler = handleClientRequest.bind(null, dataManager)
  server.addService(proto.RiskManagement.service, {
    trigger: requestHandler,
  })
  server.bind(
    `${grpc_config.host}:${grpc_config.port}`,
    grpc.ServerCredentials.createInsecure()
  )

  console.log(
    `Listening for requests from Risk Management Client on ${grpc_config.host}:${grpc_config.port}`
  )
  server.start()
}

process.on('unhandledRejection', err => {
  console.error(err)
})

const dataManager = new DataManager()
consumeEvents(dataManager)
startGRPCServer(dataManager)
