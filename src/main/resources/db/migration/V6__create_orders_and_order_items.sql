-- Flyway V6: create orders and order_item tables
CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id INTEGER NOT NULL,
    status_id INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(client_id) REFERENCES client(id),
    FOREIGN KEY(status_id) REFERENCES order_status(id)
);

CREATE TABLE IF NOT EXISTS order_item (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    FOREIGN KEY(order_id) REFERENCES orders(id),
    FOREIGN KEY(product_id) REFERENCES product(id)
);
