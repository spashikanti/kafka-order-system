import { Kafka } from 'kafkajs';    

const kafka = new Kafka({
    clientId: 'order-producer',
    brokers: ['localhost:9092']
})

const producer = kafka.producer();

const run = async () => {
    await producer.connect();

    const event = {
        type: 'OrderCreated',
        data: {
            orderId: Math.floor(Math.random() * 1000),
            item: 'Laptop',
            amount: 1200
        },
    };

    await producer.send({
        topic: 'order-events',
        messages: [
            {
                key: event.data.orderId.toString(),
                value: JSON.stringify(event)
            }
        ]
    });

    console.log(`Order event sent: ${JSON.stringify(event)}`);
    await producer.disconnect();
};

run();