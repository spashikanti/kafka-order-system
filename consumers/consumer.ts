import { Kafka } from 'kafkajs';

const kafka = new Kafka({
    clientId: 'order-consumer',
    brokers: ['localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'order-group' });
const dlqProducer = kafka.producer();

const processOrder = async (order: any) => {    
    console.log(`Processing order: ${JSON.stringify(order)}`);
    // Simulate failure randomly
    if (Math.random() < 0.3) {
        throw new Error('Random failure occurred while processing order');
    }
    
    console.log(`Order processed successfully: ${order.orderId}`);
}

const run = async () => {
    await consumer.connect();
    await dlqProducer.connect();
    await consumer.subscribe({ topic: 'order-events', fromBeginning: true });  
    
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const order = JSON.parse(message.value!.toString());

            let retries = 0;
            const maxRetries = 2;

            while (retries <= maxRetries) { 
                try {
                    await processOrder(order);
                    break; // Exit loop on success
                } catch (error) {
                    retries++;
                    console.log(`⚠️ Retry ${retries} for order ${order.orderId}`);
                    
                    if(retries > maxRetries) {
                        console.log("❌ Max retries reached. Sending to DLQ:", order);

                        await dlqProducer.send({
                            topic:"order-events-dlq",
                            messages: [
                                {
                                    key: order.orderId.toString(),
                                    value: JSON.stringify(order),
                                }
                            ],
                        });

                        console.log("➡️ Sent to DLQ topic");
                    }
                }
            }
        }
    });
};

run();