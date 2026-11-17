/**
 * ORDER SERVICE (Core Ingress Service)
 * --------------------------------------------------------------------------
 * Cung cấp 2 Endpoint để đối chiếu trực tiếp trong bài Thực nghiệm (Bước 3):
 *  1. POST /api/v1/orders/async (hoặc POST /orders):
 *     Kiến trúc Event-Driven mới với Apache Kafka. Ghi DB cục bộ -> Bắn sự kiện
 *     với Partition Key = product_id -> Trả về 202 Accepted ngay lập tức (< 35ms).
 *  2. POST /api/v1/orders/sync:
 *     Kiến trúc HTTP REST đồng bộ cũ. Treo Thread chờ Payment + Notification + Analytics,
 *     gây nghẽn Thread Pool và trả về lỗi 504 Gateway Timeout khi Flash Sale.
 */

const express = require('express');
const { TOPICS, publishEvent, murmurHash2, initKafkaInfrastructure } = require('../common/kafka-broker');
const { getInventoryState, inventoryAuditLog } = require('../inventory-service');
const { paymentStats } = require('../payment-service');
const { notificationStats } = require('../notification-service');
const { analyticsStats } = require('../analytics-service');

const app = express();
app.use(express.json());

// Cơ sở dữ liệu cục bộ của Order Service
const orderDatabase = new Map();
let orderSequence = 1000;

const orderMetrics = {
  asyncRequests: 0,
  asyncSuccess202: 0,
  syncRequests: 0,
  syncSuccess200: 0,
  syncTimeout504: 0,
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * ENDPOINT MỚI (TO-BE): BẤT ĐỒNG BỘ QUA APACHE KAFKA
 * Phản hồi 202 Accepted dưới 35ms, tách biệt hoàn toàn khỏi Payment, Notification & Analytics.
 */
async function handleAsyncOrder(req, res) {
  const startTime = process.hrtime.bigint();
  orderMetrics.asyncRequests++;

  const {
    user_id = 12345,
    product_id = 999,
    quantity = 1,
    price = 15000000,
    force_payment_fail = false,
  } = req.body || {};

  const orderId = `ORD-${++orderSequence}`;
  const productKey = String(product_id);
  const expectedPartition = murmurHash2(productKey);

  // Bước 1: Lưu trạng thái đơn hàng PENDING vào Order DB cục bộ (1-2ms)
  const orderRecord = {
    order_id: orderId,
    user_id,
    product_id: productKey,
    quantity: Number(quantity),
    price: Number(price),
    status: 'PENDING_PROCESSING',
    force_payment_fail,
    created_at: new Date().toISOString(),
  };
  orderDatabase.set(orderId, orderRecord);

  // Bước 2: Bắn sự kiện OrderCreated vào Kafka với Partition Key = product_id
  const brokerResult = await publishEvent(TOPICS.ORDER_EVENTS, productKey, orderRecord);

  const durationMs = Number(process.hrtime.bigint() - startTime) / 1e6;
  orderMetrics.asyncSuccess202++;

  // Bước 3: Trả về ngay 202 Accepted cho khách hàng (< 35ms)
  return res.status(202).json({
    status: 'ACCEPTED',
    message: 'Tôi đã nhận đơn! Đơn hàng đang được xử lý bất đồng bộ.',
    order_id: orderId,
    kafka_metadata: {
      topic: brokerResult.topic,
      partition_key: productKey,
      assigned_partition: expectedPartition,
      offset: brokerResult.offset,
    },
    response_time_ms: Number(durationMs.toFixed(2)),
  });
}

app.post('/api/v1/orders/async', handleAsyncOrder);
// Hỗ trợ đường dẫn gốc '/' và '/orders' để tương thích 100% với K6 Script mẫu ở Bước 3
app.post('/orders', handleAsyncOrder);
app.post('/', handleAsyncOrder);

/**
 * ENDPOINT CŨ (AS-IS): GIAO TIẾP ĐỒNG BỘ HTTP REST
 * Xử lý tuần tự Inventory + Payment + Notification + Analytics trước khi trả về 200 OK.
 */
app.post('/api/v1/orders/sync', async (req, res) => {
  const startTime = process.hrtime.bigint();
  orderMetrics.syncRequests++;

  const {
    user_id = 12345,
    product_id = 999,
    quantity = 1,
    price = 15000000,
    simulated_inventory_ms = 5,
    simulated_payment_ms = 15,
    simulated_notif_ms = 10,
    simulated_analytics_ms = 60,
  } = req.body || {};

  const orderId = `SYNC-ORD-${++orderSequence}`;
  orderDatabase.set(orderId, {
    order_id: orderId,
    user_id,
    product_id: String(product_id),
    quantity: Number(quantity),
    price: Number(price),
    status: 'PROCESSING_SYNC',
    created_at: new Date().toISOString(),
  });

  // Chờ tuần tự toàn bộ 4 dịch vụ hạ nguồn (cùng độ trễ với các Kafka worker)
  await sleep(simulated_inventory_ms);
  await sleep(simulated_payment_ms);
  await sleep(simulated_notif_ms);
  await sleep(simulated_analytics_ms);

  const durationMs = Number(process.hrtime.bigint() - startTime) / 1e6;
  orderMetrics.syncSuccess200++;

  return res.status(200).json({
    status: 'OK_SYNCHRONOUS',
    order_id: orderId,
    response_time_ms: Number(durationMs.toFixed(2)),
  });
});

/**
 * Endpoint theo dõi trạng thái toàn hệ thống (Metrics & Observability)
 */
app.get('/api/v1/metrics', (req, res) => {
  res.json({
    order_service: {
      total_orders_stored: orderDatabase.size,
      ...orderMetrics,
    },
    inventory_service: {
      stocks: getInventoryState(),
      recent_audit_logs: inventoryAuditLog.slice(-10),
    },
    payment_service: paymentStats,
    notification_service: notificationStats,
    analytics_service: analyticsStats,
  });
});

app.get('/health', (req, res) => {
  res.json({ status: 'UP', service: 'order-service' });
});

async function startOrderServer(port = process.env.PORT || 3000) {
  return new Promise((resolve) => {
    const server = app.listen(port, () => {
      resolve(server);
    });
  });
}

if (require.main === module) {
  (async () => {
    const info = await initKafkaInfrastructure();
    const port = process.env.PORT || 3000;
    await startOrderServer(port);
    console.log(`Order Service đang lắng nghe tại http://localhost:${port}`, info);
  })();
}

module.exports = {
  app,
  startOrderServer,
  orderMetrics,
};

