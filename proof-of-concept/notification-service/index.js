/**
 * NOTIFICATION SERVICE (Consumer Group: notification-group)
 * --------------------------------------------------------------------------
 * Giải quyết Nỗi đau 2: Gửi Email/SMS mất 1-2 giây không còn làm treo luồng đặt hàng.
 * Cơ chế:
 *  - Hoạt động trong Consumer Group độc lập `notification-group`.
 *  - Đọc cùng Topic `ecommerce.order.events` mà không cần nhân bản (Duplicate)
 *    tin nhắn ra nhiều Queue như RabbitMQ Fan-out.
 */

const { TOPICS, createConsumerGroup, initKafkaInfrastructure } = require('../common/kafka-broker');

const notificationStats = {
  sentCount: 0,
  lastSentOrderId: null,
  lastProcessedAt: null,
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function startNotificationWorker(options = { quiet: false }) {
  return createConsumerGroup('notification-group', TOPICS.ORDER_EVENTS, async ({ partition, message }) => {
    const order = JSON.parse(message.value.toString());

    // Giả lập kết nối SMTP / SMS Gateway bên ngoài (10ms trong PoC)
    await sleep(10);

    notificationStats.sentCount++;
    notificationStats.lastSentOrderId = order.order_id;
    notificationStats.lastProcessedAt = Date.now();

    if (!options.quiet) {
      console.log(
        `[Notif-Group     | Offset ${message.offset}] Đã gửi Email xác nhận cho User #${order.user_id} (Order ${order.order_id})`
      );
    }
  });
}

if (require.main === module) {
  (async () => {
    const info = await initKafkaInfrastructure();
    console.log('Notification Worker khởi động thành công:', info);
    await startNotificationWorker();
  })();
}

module.exports = {
  startNotificationWorker,
  notificationStats,
};

