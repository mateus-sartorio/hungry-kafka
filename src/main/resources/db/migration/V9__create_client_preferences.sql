-- Create client_category_preference table
CREATE TABLE IF NOT EXISTS client_category_preference (
    client_id INT NOT NULL,
    category_id INT NOT NULL,
    value FLOAT NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (client_id, category_id),
    FOREIGN KEY (client_id) REFERENCES client(id) ON DELETE CASCADE,
    FOREIGN KEY (category_id) REFERENCES category(id) ON DELETE CASCADE
);

-- Create client_product_preference table
CREATE TABLE IF NOT EXISTS client_product_preference (
    client_id INT NOT NULL,
    product_id INT NOT NULL,
    value FLOAT NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (client_id, product_id),
    FOREIGN KEY (client_id) REFERENCES client(id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES product(id) ON DELETE CASCADE
);

-- Create indexes for better query performance
CREATE INDEX idx_client_category_preference_client_id ON client_category_preference(client_id);
CREATE INDEX idx_client_category_preference_category_id ON client_category_preference(category_id);
CREATE INDEX idx_client_product_preference_client_id ON client_product_preference(client_id);
CREATE INDEX idx_client_product_preference_product_id ON client_product_preference(product_id);
