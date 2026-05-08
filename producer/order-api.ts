import express from 'express';
import { Kafka } from 'kafkajs';

const app = express();
app.use(express.json());

const kafka = new Kafka({
    clientId: 'order-service',
    brokers: ['localhost:9092'],
});

const producer = kafka.producer();

app.post('/create-order', async (req, res) => {
    const { item, amount } = req.body;

    const event = {
        type: 'OrderCreated',
        data: {
            orderId: Math.floor(Math.random() * 1000),
            item,
            amount,
        },
    };

    await producer.send({
        topic: 'order-events',
        messages: [
            {
                key: event.data.orderId.toString(),
                value: JSON.stringify(event),
            }
        ],
    });

    console.log(`Order event sent: ${JSON.stringify(event)}`);
    res.status(201).json({ message: 'Order created', orderId: event.data.orderId });
});

const start = async () => {
    await producer.connect();   
    app.listen(3000, () => {
        console.log('Order Service API running on http://localhost:3000');
    });
};

start();