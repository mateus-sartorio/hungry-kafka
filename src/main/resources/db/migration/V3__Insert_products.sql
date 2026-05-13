-- V3__Insert_products.sql
-- Insert 3 products per category

-- Hamburgers
INSERT OR IGNORE INTO product (name, description, price, photo_url, category_id) VALUES
('Classic Hamburger', 'Beef patty, lettuce, tomato, cheese, and house sauce', 12.50, 'https://images.unsplash.com/photo-1550547660-d9450f859349', (SELECT id FROM category WHERE name='Hamburger'));
INSERT OR IGNORE INTO product (name, description, price, photo_url, category_id) VALUES
('Bacon Cheeseburger', 'Grilled beef patty topped with crispy bacon and cheddar', 14.00, 'https://images.unsplash.com/photo-1550547336-3d8ecf77b99f', (SELECT id FROM category WHERE name='Hamburger'));
INSERT OR IGNORE INTO product (name, description, price, photo_url, category_id) VALUES
('Veggie Burger', 'Grilled vegetable patty with avocado and mixed greens', 11.00, 'https://images.unsplash.com/photo-1543353071-873f17a7a088', (SELECT id FROM category WHERE name='Hamburger'));

-- Pizza
INSERT OR IGNORE INTO product (name, description, price, photo_url, category_id) VALUES
('Margherita Pizza', 'Tomato, fresh mozzarella, basil, olive oil', 18.00, 'https://images.unsplash.com/photo-1548365328-8b9b7b4f6a1e', (SELECT id FROM category WHERE name='Pizza'));
INSERT OR IGNORE INTO product (name, description, price, photo_url, category_id) VALUES
('Pepperoni Pizza', 'Classic pepperoni with mozzarella and tomato sauce', 20.00, 'https://images.unsplash.com/photo-1601924582975-4f6f1f0b7a5b', (SELECT id FROM category WHERE name='Pizza'));
INSERT OR IGNORE INTO product (name, description, price, photo_url, category_id) VALUES
('Four Cheese Pizza', 'Mozzarella, gorgonzola, parmesan, and provolone', 22.00, 'https://images.unsplash.com/photo-1600891964599-f61ba0e24092', (SELECT id FROM category WHERE name='Pizza'));

-- Japanese
INSERT OR IGNORE INTO product (name, description, price, photo_url, category_id) VALUES
('Sushi Combo', 'Assorted nigiri and maki - chef''s selection', 25.00, 'https://images.unsplash.com/photo-1553621042-f6e147245754', (SELECT id FROM category WHERE name='Japanese'));
INSERT OR IGNORE INTO product (name, description, price, photo_url, category_id) VALUES
('Ramen Bowl', 'Tonkotsu-style ramen with chashu, egg, and greens', 16.50, 'https://images.unsplash.com/photo-1546069901-ba9599a7e63c', (SELECT id FROM category WHERE name='Japanese'));
INSERT OR IGNORE INTO product (name, description, price, photo_url, category_id) VALUES
('Chicken Teriyaki', 'Grilled chicken glazed with teriyaki sauce, served with rice', 14.00, 'https://images.unsplash.com/photo-1606755962770-9a9f1d8b4d9b', (SELECT id FROM category WHERE name='Japanese'));

-- Lunchbox (Marmita / Lunchbox)
INSERT OR IGNORE INTO product (name, description, price, photo_url, category_id) VALUES
('Beef Lunchbox', 'Rice, beans, grilled beef, salad and farofa', 13.00, 'https://images.unsplash.com/photo-1544025162-d76694265947', (SELECT id FROM category WHERE name='Lunchbox'));
INSERT OR IGNORE INTO product (name, description, price, photo_url, category_id) VALUES
('Chicken Lunchbox', 'Rice, beans, roasted chicken, vegetables', 12.00, 'https://images.unsplash.com/photo-1604908177522-07a2f8d2b2b0', (SELECT id FROM category WHERE name='Lunchbox'));
INSERT OR IGNORE INTO product (name, description, price, photo_url, category_id) VALUES
('Vegetarian Lunchbox', 'Rice, beans, grilled vegetables, salad', 11.50, 'https://images.unsplash.com/photo-1528756514091-dee3b6a0f2b8', (SELECT id FROM category WHERE name='Lunchbox'));

-- Desserts
INSERT OR IGNORE INTO product (name, description, price, photo_url, category_id) VALUES
('Chocolate Cake', 'Rich chocolate cake slice with ganache', 6.50, 'https://images.unsplash.com/photo-1547036967-23d11aacaee0', (SELECT id FROM category WHERE name='Desserts'));
INSERT OR IGNORE INTO product (name, description, price, photo_url, category_id) VALUES
('Cheesecake', 'Creamy cheesecake with berry compote', 7.00, 'https://images.unsplash.com/photo-1542826438-9b0e7f2d6d2b', (SELECT id FROM category WHERE name='Desserts'));
INSERT OR IGNORE INTO product (name, description, price, photo_url, category_id) VALUES
('Ice Cream Sundae', 'Vanilla ice cream with chocolate sauce and nuts', 5.50, 'https://images.unsplash.com/photo-1505250469679-203ad9ced0cb', (SELECT id FROM category WHERE name='Desserts'));
