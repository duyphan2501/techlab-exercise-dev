/**
 * ANALYTICS SERVICE (Consumer Group: analytics-bi-group)
 * --------------------------------------------------------------------------
 * Giải quyết Nỗi đau 3: Analytics Service xử lý tính toán BI nặng làm sập Core System
 * khi có Traffic Spike (Flash Sale).
 * Cơ chế:
 *  - Thuộc Consumer Group hoàn toàn độc lập (`analytics-bi-group`).
 *  - Chủ động Pull dữ liệu theo tốc độ riêng (Throttled / Batch IO).
 *  - Dù Analytics xử lý chậm 100-500ms mỗi sự kiện hoặc tạm ngưng để bảo trì,
 *    Order Service (< 35ms) và Notification Service vẫn chạy mượt mà 100%.
 *  - Hỗ trợ Data Replay: Có thể reset Offset về quá khứ để tính toán lại báo cáo BI.
 */

const { TOPICS, createConsumerGroup, initKafkaInfrastructure } = require('../common/kafka-broker');

const analyticsStats = {
  processedEvents: 0,
  orderCreatedEvents: 0,
  inventoryUpdatedEvents: 0,
  paymentSucceededEvents: 0,
  paymentFailedEvents: 0,
  totalRevenueGMV: 0,
  productSalesCount: {},
  lastProcessedAt: null,
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function startAnalyticsWorker(options = { quiet: false, simulatedHeavyIoMs: 60 }) {
  const topics = [TOPICS.ORDER_EVENTS, TOPICS.INVENTORY_EVENTS, TOPICS.PAYMENT_EVENTS];
  return createConsumerGroup('analytics-bi-group', topics, async ({ topic, message }) => {
    const payload = JSON.parse(message.value.toString());

    // Giả lập tác vụ ghi Data Warehouse / tổng hợp BI nặng tốn nhiều thời gian hơn Core Path
    await sleep(options.simulatedHeavyIoMs ?? 60);

    analyticsStats.processedEvents++;
    analyticsStats.lastProcessedAt = Date.now();

    if (topic === TOPICS.ORDER_EVENTS) {
      analyticsStats.orderCreatedEvents++;
      analyticsStats.totalRevenueGMV += Number(payload.price || 0) * Number(payload.quantity || 1);
      const pid = String(payload.product_id);
      analyticsStats.productSalesCount[pid] = (analyticsStats.productSalesCount[pid] || 0) + Number(payload.quantity || 1);
    } else if (topic === TOPICS.INVENTORY_EVENTS) {
      analyticsStats.inventoryUpdatedEvents++;
    } else if (topic === TOPICS.PAYMENT_EVENTS) {
      if (payload.event_type === 'PAYMENT_SUCCEEDED') {
        analyticsStats.paymentSucceededEvents++;
      } else {
        analyticsStats.paymentFailedEvents++;
      }
    }

    if (!options.quiet) {
      console.log(
        `[Analytics-Group | ${topic} | Offset ${message.offset}] Đã ghi BI Log cho Order ${payload.order_id} | ` +
          `Tổng GMV: ${analyticsStats.totalRevenueGMV.toLocaleString('vi-VN')} VNĐ`
      );
    }
  });
}

if (require.main === module) {
  (async () => {
    const info = await initKafkaInfrastructure();
    console.log('Analytics BI Worker khởi động thành công:', info);
    await startAnalyticsWorker();
  })();
}

module.exports = {
  startAnalyticsWorker,
  analyticsStats,
};

