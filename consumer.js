const { Kafka } = require('kafkajs')
const express = require('express')
const app = express()
 
app.get('/', function (req, res) {
  res.send('Hello World')
})

const kafka = new Kafka({
  clientId: 'my-consumer',
  brokers: ['127.0.0.1:9092', '127.0.0.1:19092', '127.0.0.1:29092']
})
 

const consumer = kafka.consumer({ groupId: 'test-group' })
const ctopic = 'animals'

const run = async () => {
  // Consuming
  await consumer.connect()
  await consumer.subscribe({ topic: ctopic, fromBeginning: true })
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        topic,
        partition,
        offset: message.offset,
        value: message.value.toString(),
      })
    },
  })
}
 
run().catch(console.error)
app.listen(3000)
console.log('starting...')