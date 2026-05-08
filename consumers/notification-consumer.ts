import { Kafka } from 'kafkajs';

const kafka = new Kafka({
    clientId: 'notification-service',
    brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'notification-group' });

const paymentNotification = async (event: any) => {
    if (event.type === 'PaymentSuccess') {
        console.log(`📢 Sending success notification for order ${event.data.orderId}: ${event.type}`);
    } else {    
        console.log(`📢 Sending failure notification for order ${event.data.orderId}: ${event.type}`);
    }
};

const run = async () => {
    await consumer.connect();
    await consumer.subscribe({ topic: 'payment-events', fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const event = JSON.parse(message.value!.toString());
            await paymentNotification(event);
        },
    });
};

run();