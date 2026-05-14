-- Flyway V7: add expectedDelivery column to orders table
ALTER TABLE orders ADD COLUMN expected_delivery TEXT;