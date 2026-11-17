/**
 * KHỞI ĐỘNG TOÀN BỘ HỆ THỐNG MICROSERVICES POC
 * --------------------------------------------------------------------------
 * Giúp Hội đồng chấm bài chỉ cần chạy `npm start` để khởi tạo đồng thời:
 *  - Kết nối cụm Apache Kafka (Docker Compose / localhost:9092)
 *  - Order Service HTTP API (Port 3000)
 *  - Inventory Worker (Consumer Group: inventory-group)
 *  - Payment Worker + Retry/DLQ Handler (Consumer Group: payment-group)
 *  - Notification Worker (Consumer Group: notification-group)
 *  - Analytics BI Worker (Consumer Group: analytics-bi-group)
 */

const { initKafkaInfrastructure, TOPICS, NUM_PARTITIONS } = require('./common/kafka-broker');
const { startOrderServer } = require('./order-service');
const { startInventoryWorker } = require('./inventory-service');
const { startPaymentWorker } = require('./payment-service');
const { startNotificationWorker } = require('./notification-service');
const { startAnalyticsWorker } = require('./analytics-service');

async function bootstrapAll() {
  console.log('================================================================================');
  console.log('KHỞI ĐỘNG HỆ THỐNG E-COMMERCE EVENT-DRIVEN ARCHITECTURE (APACHE KAFKA POC)');
  console.log('================================================================================');

  const brokerInfo = await initKafkaInfrastructure();
  console.log(`Chế độ Messaging Infrastructure : ${brokerInfo.mode}`);
  console.log(`Các Kafka Topics đã sẵn sàng    : ${Object.values(TOPICS).join(', ')} (${NUM_PARTITIONS} Partitions)`);

  await startInventoryWorker();
  await startPaymentWorker();
  await startNotificationWorker();
  await startAnalyticsWorker();

  const port = process.env.PORT || 3000;
  await startOrderServer(port);

  console.log('--------------------------------------------------------------------------------');
  console.log(`Order Service API đã sẵn sàng tại: http://localhost:${port}`);
  console.log(`   • Đặt hàng Bất đồng bộ (To-Be) : POST http://localhost:${port}/api/v1/orders/async`);
  console.log(`   • Đặt hàng Đồng bộ cũ (As-Is)  : POST http://localhost:${port}/api/v1/orders/sync`);
  console.log(`   • Xem Metrics toàn hệ thống    : GET  http://localhost:${port}/api/v1/metrics`);
  console.log('================================================================================\n');
}

if (require.main === module) {
  bootstrapAll().catch((err) => {
    console.error('Lỗi khởi động hệ thống PoC:', err);
    process.exit(1);
  });
}

module.exports = { bootstrapAll };

