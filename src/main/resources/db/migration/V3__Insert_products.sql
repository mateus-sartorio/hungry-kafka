-- V3__Insert_products.sql
-- Insert 3 products per category

-- Hamburgers
INSERT OR IGNORE INTO product (name, description, price, photo_url, category_id) VALUES
('Classic Hamburger', 'Beef patty, lettuce, tomato, cheese, and house sauce', 12.50, 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0b/RedDot_Burger.jpg/960px-RedDot_Burger.jpg', (SELECT id FROM category WHERE name='Hamburger'));
INSERT OR IGNORE INTO product (name, description, price, photo_url, category_id) VALUES
('Bacon Cheeseburger', 'Grilled beef patty topped with crispy bacon and cheddar', 14.00, 'https://upload.wikimedia.org/wikipedia/commons/thumb/4/4d/Cheeseburger.jpg/960px-Cheeseburger.jpg', (SELECT id FROM category WHERE name='Hamburger'));
INSERT OR IGNORE INTO product (name, description, price, photo_url, category_id) VALUES
('Veggie Burger', 'Grilled vegetable patty with avocado and mixed greens', 11.00, 'https://upload.wikimedia.org/wikipedia/commons/thumb/e/e0/%D7%94%D7%9E%D7%91%D7%95%D7%A8%D7%92%D7%A8_%D7%98%D7%91%D7%A2%D7%95%D7%A0%D7%99.jpg/960px-%D7%94%D7%9E%D7%91%D7%95%D7%A8%D7%92%D7%A8_%D7%98%D7%91%D7%A2%D7%95%D7%A0%D7%99.jpg', (SELECT id FROM category WHERE name='Hamburger'));

-- Pizza
INSERT OR IGNORE INTO product (name, description, price, photo_url, category_id) VALUES
('Margherita Pizza', 'Tomato, fresh mozzarella, basil, olive oil', 18.00, 'https://upload.wikimedia.org/wikipedia/commons/thumb/c/c8/Pizza_Margherita_stu_spivack.jpg/960px-Pizza_Margherita_stu_spivack.jpg', (SELECT id FROM category WHERE name='Pizza'));
INSERT OR IGNORE INTO product (name, description, price, photo_url, category_id) VALUES
('Pepperoni Pizza', 'Classic pepperoni with mozzarella and tomato sauce', 20.00, 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0c/Pepperoni_Pizza_%2829204589095%29.jpg/960px-Pepperoni_Pizza_%2829204589095%29.jpg', (SELECT id FROM category WHERE name='Pizza'));
INSERT OR IGNORE INTO product (name, description, price, photo_url, category_id) VALUES
('Four Cheese Pizza', 'Mozzarella, gorgonzola, parmesan, and provolone', 22.00, 'https://upload.wikimedia.org/wikipedia/commons/thumb/9/91/Pizza-3007395.jpg/960px-Pizza-3007395.jpg', (SELECT id FROM category WHERE name='Pizza'));

-- Japanese
INSERT OR IGNORE INTO product (name, description, price, photo_url, category_id) VALUES
('Sushi Combo', 'Assorted nigiri and maki - chef''s selection', 25.00, 'https://upload.wikimedia.org/wikipedia/commons/thumb/6/60/Sushi_platter.jpg/960px-Sushi_platter.jpg', (SELECT id FROM category WHERE name='Japanese'));
INSERT OR IGNORE INTO product (name, description, price, photo_url, category_id) VALUES
('Ramen Bowl', 'Tonkotsu-style ramen with chashu, egg, and greens', 16.50, 'https://upload.wikimedia.org/wikipedia/commons/thumb/e/ec/Shoyu_ramen%2C_at_Kasukabe_Station_%282014.05.05%29_1.jpg/960px-Shoyu_ramen%2C_at_Kasukabe_Station_%282014.05.05%29_1.jpg', (SELECT id FROM category WHERE name='Japanese'));
INSERT OR IGNORE INTO product (name, description, price, photo_url, category_id) VALUES
('Chicken Teriyaki', 'Grilled chicken glazed with teriyaki sauce, served with rice', 14.00, 'https://upload.wikimedia.org/wikipedia/commons/thumb/9/91/22nd_June_2012_Teriyaki_Duck.jpg/960px-22nd_June_2012_Teriyaki_Duck.jpg', (SELECT id FROM category WHERE name='Japanese'));

-- Lunchbox
INSERT OR IGNORE INTO product (name, description, price, photo_url, category_id) VALUES
('Beef Lunchbox', 'Rice, beans, grilled beef, salad and farofa', 13.00, 'https://upload.wikimedia.org/wikipedia/commons/thumb/a/a8/NCI_Visuals_Food_Meal_Lunch.jpg/960px-NCI_Visuals_Food_Meal_Lunch.jpg', (SELECT id FROM category WHERE name='Lunchbox'));
INSERT OR IGNORE INTO product (name, description, price, photo_url, category_id) VALUES
('Chicken Lunchbox', 'Rice, beans, roasted chicken, vegetables', 12.00, 'https://upload.wikimedia.org/wikipedia/commons/thumb/d/d9/Max%27s_Roasted_Chicken_-_Evan_Swigart.jpg/960px-Max%27s_Roasted_Chicken_-_Evan_Swigart.jpg', (SELECT id FROM category WHERE name='Lunchbox'));
INSERT OR IGNORE INTO product (name, description, price, photo_url, category_id) VALUES
('Vegetarian Lunchbox', 'Rice, beans, grilled vegetables, salad', 11.50, 'https://upload.wikimedia.org/wikipedia/commons/thumb/9/94/Salad_platter.jpg/960px-Salad_platter.jpg', (SELECT id FROM category WHERE name='Lunchbox'));

-- Desserts
INSERT OR IGNORE INTO product (name, description, price, photo_url, category_id) VALUES
('Chocolate Cake', 'Rich chocolate cake slice with ganache', 6.50, 'https://upload.wikimedia.org/wikipedia/commons/0/04/Pound_layer_cake.jpg', (SELECT id FROM category WHERE name='Desserts'));
INSERT OR IGNORE INTO product (name, description, price, photo_url, category_id) VALUES
('Cheesecake', 'Creamy cheesecake with berry compote', 7.00, 'https://upload.wikimedia.org/wikipedia/commons/thumb/e/ea/Baked_cheesecake_with_raspberries_and_blueberries.jpg/960px-Baked_cheesecake_with_raspberries_and_blueberries.jpg', (SELECT id FROM category WHERE name='Desserts'));
INSERT OR IGNORE INTO product (name, description, price, photo_url, category_id) VALUES
('Ice Cream Sundae', 'Vanilla ice cream with chocolate sauce and nuts', 5.50, 'https://upload.wikimedia.org/wikipedia/commons/a/ae/StrawberrySundae.jpg', (SELECT id FROM category WHERE name='Desserts'));
