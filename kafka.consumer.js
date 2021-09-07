require('dotenv').config()
const { Kafka } = require('kafkajs')
const kafka = new Kafka({
  clientId: process.env.CLIENT_ID,
  brokers: process.env.KAFKA_HOSTS.split(',')
})

const getMessages = async () => {
  // create a consumer instance
  const consumer = kafka.consumer({ groupId: process.env.GROUP_ID })
  
  // connect to the consumer
  await consumer.connect()

  // suscribe to the topic you want to get messages from
  await consumer.subscribe({ 
    topic: process.env.TOPIC, 
    fromBeginning: true // this is optional
  })

  // start receiving messages
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        value: message.value.toString()
      })
    }
  })
}

getMessages()