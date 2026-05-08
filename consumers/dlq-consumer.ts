import { Kafka } from "kafkajs";

const kafka = new Kafka({
    clientId: "dlq-consumer",
    brokers: ["localhost:9092"]
});

const consumer = kafka.consumer({ groupId: "dlq-group" });

const run = async () => {
    await consumer.connect();
    await consumer.subscribe({ topic: "order-events-dlq", fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const order = JSON.parse(message.value!.toString());
            console.log("💀 DLQ received failed order:", order);
        }
    });
}

run();