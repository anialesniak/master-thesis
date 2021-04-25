const _ = require('lodash')

const DataManager = require('./data-manager')
const dataManager = new DataManager()

module.exports = class MessageBuffer {
  constructor(order, timeout) {
    this.messageQueue = []
    this.order = order
    setInterval(this.publishFromBuffer.bind(this), timeout);
  }

  handleEvent(event) {
    console.log('An event has been consumed:')
    console.log(JSON.stringify(event, null, 4))
    dataManager.addEvent(event)
    dataManager
      .save()
      .catch(error => {
        console.error(`Error: ${error}`)
      })
  }

  publishFromBuffer() {
    console.log("Publish has been initiated.")
    const messages = this.messageQueue
    this.messageQueue = []

    const events = _.chain(messages)
      .groupBy(message => message.key)
      .values()
      .flatMap(array => _.sortBy(array, (message) => this.order.indexOf(message.value.kind)))
      .map(message => message.value)
      .value()

    events.forEach(event => this.handleEvent(event))
  }

  bufferMessage(message) {
    const messageValue = JSON.parse(message.value)
    const keyValueMessage = {key: message.key.toString(), value: messageValue}
    this.messageQueue.push(keyValueMessage)
  }
}
