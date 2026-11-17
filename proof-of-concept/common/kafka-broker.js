/**
 * Module kết nối Apache Kafka (KafkaJS) kèm cơ chế tính toán Partition (MurmurHash2)
 * Yêu cầu cụm Apache Kafka đang chạy (Docker Compose / K8s) tại KAFKA_BROKERS (mặc định localhost:9092).
 */

const { Kafka, logLevel } = require('kafkajs');

const TOPICS = {
  ORDER_EVENTS: 'ecommerce.order.events',
  INVENTORY_EVENTS: 'ecommerce.inventory.events',
  PAYMENT_EVENTS: 'ecommerce.payment.events',
  PAYMENT_RETRY: 'ecommerce.payment.retry',
  PAYMENT_DLQ: 'ecommerce.payment.dlq',
};

const NUM_PARTITIONS = 3;

/**
 * Thuật toán băm MurmurHash2 tiêu chuẩn của Apache Kafka
 * Chứng minh công thức: Partition = abs(MurmurHash2(product_id)) % NUM_PARTITIONS
 */
function murmurHash2(keyString, numPartitions = NUM_PARTITIONS) {
  const data = Buffer.from(String(keyString), 'utf8');
  const length = data.length;
  const seed = 0x9747b28c;
  const m = 0x5bd1e995;
  const r = 24;

  let h = seed ^ length;
  const length4 = Math.floor(length / 4);

  for (let i = 0; i < length4; i++) {
    const i4 = i * 4;
    let k =
      (data[i4 + 0] & 0xff) |
      ((data[i4 + 1] & 0xff) << 8) |
      ((data[i4 + 2] & 0xff) << 16) |
      ((data[i4 + 3] & 0xff) << 24);
    k = Math.imul(k, m);
    k ^= k >>> r;
    k = Math.imul(k, m);
    h = Math.imul(h, m);
    h ^= k;
  }

  const remaining = length % 4;
  if (remaining === 3) h ^= (data[(length & ~3) + 2] & 0xff) << 16;
  if (remaining >= 2) h ^= (data[(length & ~3) + 1] & 0xff) << 8;
  if (remaining >= 1) {
    h ^= data[length & ~3] & 0xff;
    h = Math.imul(h, m);
  }

  h ^= h >>> 13;
  h = Math.imul(h, m);
  h ^= h >>> 15;

  // toPositive trong Kafka Java client: h & 0x7fffffff
  const positiveHash = h & 0x7fffffff;
  return positiveHash % numPartitions;
}

let kafkaClient = null;
let sharedProducer = null;

async function initKafkaInfrastructure() {
  const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
  kafkaClient = new Kafka({
    clientId: 'ecommerce-poc',
    brokers,
    logLevel: logLevel.NOTHING,
    connectionTimeout: 3000,
    retry: { initialRetryTime: 200, retries: 3 },
  });

  try {
    const admin = kafkaClient.admin();
    await admin.connect();
    const existingTopics = await admin.listTopics();

    const topicsToCreate = Object.values(TOPICS)
      .filter((t) => !existingTopics.includes(t))
      .map((t) => ({
        topic: t,
        numPartitions: NUM_PARTITIONS,
        replicationFactor: 1,
      }));

    if (topicsToCreate.length > 0) {
      await admin.createTopics({ topics: topicsToCreate, waitForLeaders: true });
    }
    await admin.disconnect();

    sharedProducer = kafkaClient.producer();
    await sharedProducer.connect();
    return { mode: 'APACHE_KAFKA_CLUSTER', brokers };
  } catch (err) {
    throw new Error(
      `Không thể kết nối Apache Kafka tại [${brokers.join(', ')}]: ${err.message}. ` +
        `Hãy khởi động Kafka trước bằng lệnh: docker compose -f deployments/docker-compose.yml up -d`
    );
  }
}

async function publishEvent(topic, key, payload, headers = {}) {
  if (!sharedProducer) {
    throw new Error('Kafka Producer chưa được khởi tạo. Hãy gọi initKafkaInfrastructure() trước.');
  }

  const partition = key ? murmurHash2(key, NUM_PARTITIONS) : 0;
  const res = await sharedProducer.send({
    topic,
    messages: [
      {
        key: key ? String(key) : null,
        partition,
        value: JSON.stringify(payload),
        headers,
      },
    ],
  });

  return {
    topic,
    partition: res[0].partition ?? partition,
    offset: res[0].baseOffset ?? '0',
    mode: 'KAFKA',
  };
}

async function resetGroupOffsetsToLatest(groupId, topics) {
  if (!kafkaClient) return;
  const topicList = Array.isArray(topics) ? topics : [topics];
  const admin = kafkaClient.admin();
  await admin.connect();
  try {
    for (const topic of topicList) {
      try {
        await admin.resetOffsets({ groupId, topic, earliest: false });
      } catch (_) {
        // Nhóm chưa tồn tại hoặc chưa có offset -> fromBeginning: false sẽ tự đọc từ latest
      }
    }
  } finally {
    await admin.disconnect();
  }
}

async function createConsumerGroup(groupId, topics, eachMessageHandler) {
  if (!kafkaClient) {
    throw new Error('Kafka Client chưa được khởi tạo. Hãy gọi initKafkaInfrastructure() trước.');
  }

  const topicList = Array.isArray(topics) ? topics : [topics];
  const consumer = kafkaClient.consumer({ groupId });
  await consumer.connect();
  for (const topic of topicList) {
    await consumer.subscribe({ topic, fromBeginning: false });
  }

  const joined = new Promise((resolve) => {
    const remove = consumer.on(consumer.events.GROUP_JOIN, () => {
      remove();
      resolve();
    });
  });

  await consumer.run({
    eachMessage: eachMessageHandler,
  });
  await joined;
  return consumer;
}

module.exports = {
  TOPICS,
  NUM_PARTITIONS,
  murmurHash2,
  initKafkaInfrastructure,
  publishEvent,
  createConsumerGroup,
  resetGroupOffsetsToLatest,
};

