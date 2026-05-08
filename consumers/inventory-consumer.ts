import { Kafka } from "kafkajs";

const kafka = new Kafka({
    clientId: "inventory-service",
    brokers: ["localhost:9092"]
});

const consumer = kafka.consumer({ groupId: "inventory-group" });

const processInventory = async (order: any) => {
    console.log(`Processing inventory for order: ${JSON.stringify(order)}`);
};

const run = async () => {
    await consumer.connect();
    await consumer.subscribe({ topic: "payment-events", fromBeginning: true});

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const event = JSON.parse(message.value!.toString());
            if (event.type === "PaymentSuccess") {
                await processInventory(event.data);
            } else {
                return; // Ignore payment failures
            }
        }
    });
};

run();