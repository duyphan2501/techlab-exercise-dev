/**
 * KỊCH BẢN BENCHMARK THỰC NGHIỆM TỰ ĐỘNG (NODE.JS EMPIRICAL LOAD TEST)
 * --------------------------------------------------------------------------
 * Giúp Hội đồng Kỹ thuật kiểm chứng ngay lập tức bảng số liệu Thực nghiệm ở Bước 3
 * chỉ bằng lệnh `npm run benchmark` mà không cần cài đặt thêm công cụ bên ngoài.
 *
 * Kịch bản đối chiếu:
 *  1. Hệ thống cũ (HTTP Đồng bộ - /api/v1/orders/sync):
 *     Gửi 40 requests đồng thời trong đợt Flash Sale.
 *  2. Hệ thống mới (Apache Kafka Bất đồng bộ - /api/v1/orders/async):
 *     Gửi 100 requests đồng thời trong đợt Flash Sale.
 */

const http = require('http');
const { initKafkaInfrastructure } = require('../proof-of-concept/common/kafka-broker');
const { startOrderServer } = require('../proof-of-concept/order-service');
const { startInventoryWorker } = require('../proof-of-concept/inventory-service');
const { startPaymentWorker } = require('../proof-of-concept/payment-service');
const { startNotificationWorker } = require('../proof-of-concept/notification-service');
const { startAnalyticsWorker } = require('../proof-of-concept/analytics-service');

const keepAliveAgent = new http.Agent({ keepAlive: true, maxSockets: 100 });

function sendPostRequest(port, path, bodyObj, timeoutMs = 5000) {
  return new Promise((resolve) => {
    const start = process.hrtime.bigint();
    const data = JSON.stringify(bodyObj);

    const req = http.request(
      {
        hostname: '127.0.0.1',
        port,
        path,
        method: 'POST',
        timeout: timeoutMs,
        agent: keepAliveAgent,
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(data),
        },
      },
      (res) => {
        let raw = '';
        res.on('data', (chunk) => (raw += chunk));
        res.on('end', () => {
          const durationMs = Number(process.hrtime.bigint() - start) / 1e6;
          resolve({ statusCode: res.statusCode, durationMs, body: raw });
        });
      }
    );

    req.on('timeout', () => {
      req.destroy();
      const durationMs = Number(process.hrtime.bigint() - start) / 1e6;
      resolve({ statusCode: 504, durationMs, body: null });
    });

    req.on('error', () => {
      const durationMs = Number(process.hrtime.bigint() - start) / 1e6;
      resolve({ statusCode: 500, durationMs, body: null });
    });

    req.write(data);
    req.end();
  });
}

function calculatePercentile(sortedArr, percentile) {
  if (sortedArr.length === 0) return 0;
  const index = Math.ceil((percentile / 100) * sortedArr.length) - 1;
  return sortedArr[Math.max(0, index)];
}

async function runLoadBatch(port, path, concurrency, totalRequests, extraPayload = {}, timeoutMs = 5000) {
  const results = [];
  const startWall = process.hrtime.bigint();
  const productPool = [999, 999, 888, 777]; // 50% sản phẩm hot 999, 50% các sản phẩm khác

  let index = 0;
  async function worker() {
    while (index < totalRequests) {
      const currentIdx = ++index;
      const res = await sendPostRequest(
        port,
        path,
        {
          user_id: 10000 + currentIdx,
          product_id: productPool[currentIdx % productPool.length],
          quantity: 1,
          price: 15000000,
          ...extraPayload,
        },
        timeoutMs
      );
      results.push(res);
    }
  }

  const workers = Array.from({ length: concurrency }, () => worker());
  await Promise.all(workers);

  const totalWallSeconds = Number(process.hrtime.bigint() - startWall) / 1e9;
  const durations = results.map((r) => r.durationMs).sort((a, b) => a - b);
  const avgLatency = durations.reduce((a, b) => a + b, 0) / durations.length;
  const p95Latency = calculatePercentile(durations, 95);
  const errorCount = results.filter((r) => r.statusCode >= 400).length;
  const errorRate = (errorCount / results.length) * 100;
  const rps = results.length / totalWallSeconds;

  return {
    totalRequests: results.length,
    avgLatency: avgLatency.toFixed(2),
    p95Latency: p95Latency.toFixed(2),
    rps: rps.toFixed(1),
    errorCount,
    errorRate: errorRate.toFixed(1),
  };
}

async function main() {
  console.log('================================================================================');
  console.log('THỰC NGHIỆM SO SÁNH HIỆU NĂNG (SYNC HTTP vs ASYNC KAFKA)');
  console.log('================================================================================\n');

  await initKafkaInfrastructure();
  await startInventoryWorker({ quiet: true });
  await startPaymentWorker({ quiet: true });
  await startNotificationWorker({ quiet: true });
  await startAnalyticsWorker({ quiet: true });

  const testPort = 3099;
  const server = await startOrderServer(testPort);

  const concurrency = 50;
  const totalRequests = 200;

  console.log(`1A. Kịch bản Tải bình thường - HTTP Đồng bộ (${totalRequests} reqs, concurrency ${concurrency})...`);
  const syncStats = await runLoadBatch(testPort, '/api/v1/orders/sync', concurrency, totalRequests, {
    simulated_inventory_ms: 5,
    simulated_payment_ms: 15,
    simulated_notif_ms: 10,
    simulated_analytics_ms: 60,
  });

  console.log(`1B. Kịch bản Tải bình thường - Kafka Bất đồng bộ (${totalRequests} reqs, concurrency ${concurrency})...`);
  const asyncStats = await runLoadBatch(testPort, '/api/v1/orders/async', concurrency, totalRequests);

  console.log('2A. Kịch bản Payment chậm 2000ms (HTTP timeout 1500ms) - HTTP Đồng bộ (50 reqs, concurrency 25)...');
  const syncSlowStats = await runLoadBatch(
    testPort,
    '/api/v1/orders/sync',
    25,
    50,
    {
      simulated_inventory_ms: 5,
      simulated_payment_ms: 2000,
      simulated_notif_ms: 10,
      simulated_analytics_ms: 60,
    },
    1500
  );

  console.log('2B. Kịch bản Payment chậm 2000ms (HTTP timeout 1500ms) - Kafka Bất đồng bộ (50 reqs, concurrency 25)...\n');
  const asyncSlowStats = await runLoadBatch(testPort, '/api/v1/orders/async', 25, 50, {}, 1500);

  console.log('================================================================================');
  console.log('KẾT QUẢ THỰC NGHIỆM ĐỐI CHIẾU');
  console.log('================================================================================');
  console.table([
    {
      'Kịch bản / Chỉ số': '[Bình thường] Avg Latency (ms)',
      'Hệ thống cũ (HTTP Đồng bộ)': `${syncStats.avgLatency} ms`,
      'Hệ thống mới (Kafka Event-Driven)': `${asyncStats.avgLatency} ms`,
    },
    {
      'Kịch bản / Chỉ số': '[Bình thường] P95 Latency (ms)',
      'Hệ thống cũ (HTTP Đồng bộ)': `${syncStats.p95Latency} ms`,
      'Hệ thống mới (Kafka Event-Driven)': `${asyncStats.p95Latency} ms`,
    },
    {
      'Kịch bản / Chỉ số': '[Bình thường] Throughput (RPS)',
      'Hệ thống cũ (HTTP Đồng bộ)': `${syncStats.rps} RPS`,
      'Hệ thống mới (Kafka Event-Driven)': `${asyncStats.rps} RPS`,
    },
    {
      'Kịch bản / Chỉ số': '[Payment chậm 2s] Tỷ lệ Timeout',
      'Hệ thống cũ (HTTP Đồng bộ)': `${syncSlowStats.errorRate}% (${syncSlowStats.errorCount}/${syncSlowStats.totalRequests})`,
      'Hệ thống mới (Kafka Event-Driven)': `${asyncSlowStats.errorRate}% (${asyncSlowStats.errorCount}/${asyncSlowStats.totalRequests})`,
    },
    {
      'Kịch bản / Chỉ số': '[Payment chậm 2s] Avg Latency (ms)',
      'Hệ thống cũ (HTTP Đồng bộ)': `${syncSlowStats.avgLatency} ms`,
      'Hệ thống mới (Kafka Event-Driven)': `${asyncSlowStats.avgLatency} ms`,
    },
  ]);

  server.close();
  process.exit(0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

