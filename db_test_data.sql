-- Добавление пользователей
INSERT INTO users (name, email) VALUES ('John Doe', 'john@example.com');
INSERT INTO users (name, email) VALUES ('Jane Smith', 'jane@example.com');
INSERT INTO users (name, email) VALUES ('Alice Johnson', 'alice@example.com');
INSERT INTO users (name, email) VALUES ('Bob Brown', 'bob@example.com');

-- Добавление заказов
INSERT INTO orders (user_id, product_name, quantity) VALUES (1, 'Product A', 2);
INSERT INTO orders (user_id, product_name, quantity) VALUES (1, 'Product B', 1);
INSERT INTO orders (user_id, product_name, quantity) VALUES (2, 'Product C', 5);
INSERT INTO orders (user_id, product_name, quantity) VALUES (3, 'Product D', 3);
INSERT INTO orders (user_id, product_name, quantity) VALUES (4, 'Product E', 4);

-- Добавление секретных данных
INSERT INTO topsecret (user_id, user_secret_info) VALUES (1, 'Secret Data 1');
INSERT INTO topsecret (user_id, user_secret_info) VALUES (2, 'Secret Data 2');
INSERT INTO topsecret (user_id, user_secret_info) VALUES (3, 'Secret Data 3');
INSERT INTO topsecret (user_id, user_secret_info) VALUES (4, 'Secret Data 4');

-- Проверка
SELECT * FROM users;
SELECT * FROM orders;
SELECT * FROM topsecret;
