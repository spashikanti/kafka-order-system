import { Kafka } from "kafkajs";

const kafka = new Kafka({
    clientId: "payment-service",
    brokers: ["localhost:9092"]
});

const consumer = kafka.consumer({ groupId: "payment-group" });
const producer = kafka.producer();

const processPayment = async (order: any) => {
    console.log(`Processing payment for order: ${JSON.stringify(order)}`);  

    // Simulate payment processing failure randomly
    if (Math.random() < 0.7) {
        return "PAYMENT SUCCESS";
    }
    return "PAYMENT FAILURE";    
};

const run = async () => {
    await consumer.connect();
    await consumer.subscribe({ topic: "order-events", fromBeginning: true });
    await producer.connect();

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const event = JSON.parse(message.value!.toString());
            if (event.type !== "OrderCreated") return;

            const paymentResult = await processPayment(event.order);

            const paymentEvent = {
                type: paymentResult === "PAYMENT SUCCESS" ? "PaymentSuccess" : "PaymentFailed",
                data: event.data,
            };

            await producer.send({
                topic: "payment-events",
                messages: [
                    {
                        key: paymentEvent.data.orderId.toString(),
                        value: JSON.stringify(paymentEvent)
                    },
                ],
            });

            console.log(`Payment event sent: ${JSON.stringify(paymentEvent)}`);
        },
    });
};

run();
