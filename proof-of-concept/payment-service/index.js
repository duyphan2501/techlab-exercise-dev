/**
 * PAYMENT SERVICE (Consumer Group: payment-group)
 * --------------------------------------------------------------------------
 * Giải quyết Nỗi đau 1 & 5:
 *  - Nỗi đau 1: Tách việc gọi Cổng thanh toán bên thứ 3 ra khỏi luồng HTTP của Order Service.
 *  - Nỗi đau 5: Chuẩn hóa cơ chế Retry tập trung qua Topic `ecommerce.payment.retry`
 *    kèm Exponential Backoff và chuyển vào `ecommerce.payment.dlq` (Dead Letter Queue)
 *    khi vượt quá số lần thử tối đa (MAX_RETRIES = 3).
 */

const { TOPICS, createConsumerGroup, publishEvent, initKafkaInfrastructure } = require('../common/kafka-broker');

const MAX_RETRIES = 3;
const paymentStats = {
  processed: 0,
  succeeded: 0,
  retried: 0,
  dlqCount: 0,
  dlqMessages: [],
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function startPaymentWorker(options = { quiet: false, simulatedFailureRate: 0.15 }) {
  // 1. Lắng nghe sự kiện đơn hàng mới
  await createConsumerGroup('payment-group', TOPICS.ORDER_EVENTS, async ({ partition, message }) => {
    const order = JSON.parse(message.value.toString());
    await processPayment(order, 0, partition, options);
  });

  // 2. Lắng nghe Topic Retry để xử lý lại các giao dịch lỗi tạm thời
  await createConsumerGroup('payment-retry-group', TOPICS.PAYMENT_RETRY, async ({ partition, message }) => {
    const retryPayload = JSON.parse(message.value.toString());
    const attempt = retryPayload.retry_attempt || 1;

    // Mô phỏng Exponential Backoff ngắn trong PoC (20ms * 2^attempt)
    const backoffMs = 20 * Math.pow(2, attempt);
    await sleep(backoffMs);

    await processPayment(retryPayload, attempt, partition, options);
  });
}

async function processPayment(order, attempt, partition, options) {
  paymentStats.processed++;

  // Giả lập gọi Cổng thanh toán bên thứ 3
  await sleep(15);

  const shouldFail =
    order.force_payment_fail === true ||
    (order.force_payment_fail !== false && Math.random() < (options.simulatedFailureRate ?? 0.15));

  if (!shouldFail) {
    paymentStats.succeeded++;
    await publishEvent(TOPICS.PAYMENT_EVENTS, order.product_id, {
      event_type: 'PAYMENT_SUCCEEDED',
      order_id: order.order_id,
      product_id: order.product_id,
      price: order.price,
      attempt,
    });
    if (!options.quiet) {
      console.log(
        `[Payment-Group   | Partition ${partition}] Order ${order.order_id} -> ` +
          `Thanh toán THÀNH CÔNG (${order.price?.toLocaleString('vi-VN')} VNĐ)${attempt > 0 ? ` sau lần thử lại #${attempt}` : ''}`
      );
    }
    return;
  }

  // Xử lý khi cổng thanh toán đối tác bị lỗi/timeout
  if (attempt < MAX_RETRIES) {
    const nextAttempt = attempt + 1;
    paymentStats.retried++;
    if (!options.quiet) {
      console.warn(
        `[Payment-Group   | Retry #${nextAttempt}/${MAX_RETRIES}] Order ${order.order_id} gặp lỗi Cổng thanh toán -> Đẩy vào ${TOPICS.PAYMENT_RETRY}`
      );
    }
    await publishEvent(TOPICS.PAYMENT_RETRY, order.product_id, {
      ...order,
      retry_attempt: nextAttempt,
      last_error: 'GATEWAY_TIMEOUT_504',
      retried_at: new Date().toISOString(),
    });
  } else {
    paymentStats.dlqCount++;
    const dlqRecord = {
      ...order,
      failed_attempts: attempt,
      final_status: 'MOVED_TO_DLQ',
      reason: 'PARTNER_GATEWAY_UNAVAILABLE_AFTER_MAX_RETRIES',
      failed_at: new Date().toISOString(),
    };
    paymentStats.dlqMessages.push(dlqRecord);

    if (!options.quiet) {
      console.error(
        `[Payment-DLQ     | Cảnh báo] Order ${order.order_id} thất bại sau ${MAX_RETRIES} lần thử -> Lưu trữ tập trung tại ${TOPICS.PAYMENT_DLQ}`
      );
    }
    await publishEvent(TOPICS.PAYMENT_DLQ, order.product_id, dlqRecord);
    await publishEvent(TOPICS.PAYMENT_EVENTS, order.product_id, {
      event_type: 'PAYMENT_FAILED',
      order_id: order.order_id,
      product_id: order.product_id,
      reason: dlqRecord.reason,
    });
  }
}

if (require.main === module) {
  (async () => {
    const info = await initKafkaInfrastructure();
    console.log('Payment Worker khởi động thành công:', info);
    await startPaymentWorker();
  })();
}

module.exports = {
  startPaymentWorker,
  paymentStats,
};

