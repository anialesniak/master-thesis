const _ = require('lodash')
const fs = require('fs')

const DataManager = require('./data-manager')
const dataManager = new DataManager()

module.exports = class MessageBuffer {
  constructor(order, timeout) {
    this.messageQueue = []
    this.order = order
    setInterval(this.publishFromBuffer.bind(this), timeout);
  }

  handleEvent(event) {
    const line = `${event.value.originator} ${event.value.date} ${new Date().getTime()} ${event.key} ${event.value.kind} \n`
    fs.appendFile('received-events-baseline.txt', line, function (err) {})

    dataManager.addEvent(event.value)
    dataManager
      .save()
      .catch(error => {
        console.error(`Error: ${error}`)
      })
  }

  publishFromBuffer() {
    console.log(`Publish has been initiated with ${this.messageQueue.length} messages`)
    const messages = this.messageQueue
    this.messageQueue = []

    const events = _.chain(messages)
      .groupBy(message => message.key)
      .values()
      .flatMap(array => {
        const sorted = _.sortBy(array, (message) => this.order.indexOf(message.value.kind))
        const index = sorted.findIndex(event => event.value.kind === "DeletePolicyEvent")
        return sorted.slice(0, index !== -1 ? index + 1 : array.length)
      })
      .value()

    events.forEach(event => this.handleEvent(event))
  }

  bufferMessage(message) {
    const messageValue = JSON.parse(message.value)
    const keyValueMessage = {key: message.key.toString(), value: messageValue}
    this.messageQueue.push(keyValueMessage)
  }
}
