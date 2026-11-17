/**
 * INVENTORY SERVICE (Consumer Group: inventory-group)
 * --------------------------------------------------------------------------
 * Giải quyết Nỗi đau: Tranh chấp dữ liệu (Race Condition) khi Flash Sale.
 * Cơ chế:
 *  - Order Service đặt Partition Key = product_id.
 *  - Kafka băm MurmurHash2(product_id) % NUM_PARTITIONS để đưa mọi đơn hàng
 *    của cùng 1 sản phẩm (VD: iPhone 16 - ID: 999) vào đúng 1 Partition duy nhất.
 *  - Inventory Worker đọc tuần tự (FIFO) trên Partition đó -> Không bao giờ xảy ra
 *    2 luồng cùng đọc/ghi một mặt hàng tại cùng một thời điểm -> Loại bỏ hoàn toàn
 *    Race Condition mà không cần Distributed Lock (Redis Lock).
 */

const { TOPICS, createConsumerGroup, publishEvent, initKafkaInfrastructure } = require('../common/kafka-broker');

// Kho hàng giả lập trong Database của Inventory Service
const inventoryDatabase = new Map([
  ['999', { name: 'iPhone 16 Pro Max', stock: 500, reserved: 0 }],
  ['888', { name: 'AirPods Pro Gen 2', stock: 300, reserved: 0 }],
  ['777', { name: 'MacBook Air M3', stock: 150, reserved: 0 }],
]);

// Lưu lịch sử xử lý để kiểm chứng thực nghiệm (Proof 1)
const inventoryAuditLog = [];

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function startInventoryWorker(options = { quiet: false }) {
  return createConsumerGroup('inventory-group', TOPICS.ORDER_EVENTS, async ({ partition, message }) => {
    const payload = JSON.parse(message.value.toString());
    const productId = String(payload.product_id);
    const quantity = Number(payload.quantity || 1);

    if (!inventoryDatabase.has(productId)) {
      inventoryDatabase.set(productId, { name: `Product #${productId}`, stock: 100, reserved: 0 });
    }

    const item = inventoryDatabase.get(productId);
    const stockBefore = item.stock;

    // Giả lập độ trễ truy vấn & cập nhật DB (5ms)
    // Nếu không có Kafka Partition Key tuần tự, 20 request đồng thời sẽ đọc cùng stockBefore và gây Race Condition!
    await sleep(5);

    let status = 'RESERVED';
    if (item.stock >= quantity) {
      item.stock -= quantity;
      item.reserved += quantity;
    } else {
      status = 'OUT_OF_STOCK';
    }

    const logEntry = {
      order_id: payload.order_id,
      product_id: productId,
      partition,
      offset: message.offset,
      stock_before: stockBefore,
      stock_after: item.stock,
      status,
      processed_at: Date.now(),
    };
    inventoryAuditLog.push(logEntry);

    await publishEvent(TOPICS.INVENTORY_EVENTS, productId, {
      event_type: 'INVENTORY_UPDATED',
      ...logEntry,
    });

    if (!options.quiet) {
      console.log(
        `[Inventory-Group | Partition ${partition} | Offset ${message.offset}] ` +
          `Order ${payload.order_id} (SP: ${productId}) -> Tồn kho: ${stockBefore} -> ${item.stock} [${status}]`
      );
    }
  });
}

function getInventoryState(productId) {
  if (productId) {
    return inventoryDatabase.get(String(productId)) || null;
  }
  return Object.fromEntries(inventoryDatabase.entries());
}

function resetInventoryProduct(productId, initialStock = 50) {
  inventoryDatabase.set(String(productId), {
    name: `Flash Sale Product #${productId}`,
    stock: initialStock,
    reserved: 0,
  });
  inventoryAuditLog.length = 0;
}

if (require.main === module) {
  (async () => {
    const info = await initKafkaInfrastructure();
    console.log('Inventory Worker khởi động thành công:', info);
    await startInventoryWorker();
  })();
}

module.exports = {
  startInventoryWorker,
  getInventoryState,
  resetInventoryProduct,
  inventoryAuditLog,
};

