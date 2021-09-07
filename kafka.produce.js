require('dotenv').config()
const { Kafka } = require('kafkajs')
const kafka = new Kafka({
  clientId: process.env.CLIENT_ID,
  brokers: process.env.KAFKA_HOSTS.split(',')
})

const sendMessage = async (message) => {
  try {
    console.log('starting')

  // create a producer instance
  const producer = kafka.producer()
  console.log('producer created')

  // connect to the producer instance
  await producer.connect()
  console.log('producer connected')
  
  // send a message
  await producer.send({
    topic: process.env.TOPIC,
    messages: [
      { value: message }
    ]
  })
  console.log('producer sent message')

  // disconnect after finishing
  await producer.disconnect()
  console.log('producer disconnected')
  } catch (error) {
    console.log(error)
  }
}

let count = 0
setInterval(()=>{
  sendMessage('hello world count='+count)
  count++
}, 2000)

