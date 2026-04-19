CREATE DATABASE IF NOT EXISTS milk_collection;

USE milk_collection;

CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    phone VARCHAR(20) NOT NULL UNIQUE,
    role ENUM('admin', 'operator', 'farmer') NOT NULL,
    center_id INT
);

CREATE TABLE IF NOT EXISTS milk_records (
    id INT AUTO_INCREMENT PRIMARY KEY,
    farmer_id INT NOT NULL,
    date DATE NOT NULL,
    `session` ENUM('FN', 'AN') NOT NULL DEFAULT 'FN',
    litres DECIMAL(10, 2) NOT NULL,
    fat DECIMAL(10, 2) NOT NULL,
    base_rate DECIMAL(10, 2) NOT NULL DEFAULT 0,
    deduction DECIMAL(10, 2) NOT NULL DEFAULT 0,
    final_rate DECIMAL(10, 2) NOT NULL DEFAULT 0,
    amount DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (farmer_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS milk_rates (
    id INT AUTO_INCREMENT PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    base_rate DECIMAL(10, 2) NOT NULL
);

INSERT INTO users (name, phone, role, center_id)
SELECT 'System Admin', '9999999999', 'admin', 1
WHERE NOT EXISTS (
    SELECT 1 FROM users WHERE phone = '9999999999'
);

INSERT INTO users (name, phone, role, center_id)
SELECT 'Milk Operator', '8888888888', 'operator', 1
WHERE NOT EXISTS (
    SELECT 1 FROM users WHERE phone = '8888888888'
);

-- Existing database upgrade queries if you are not using app.py startup migration:
-- ALTER TABLE milk_records ADD COLUMN `session` ENUM('FN', 'AN') NOT NULL DEFAULT 'FN' AFTER date;
-- ALTER TABLE milk_records ADD COLUMN base_rate DECIMAL(10, 2) NOT NULL DEFAULT 0 AFTER fat;
-- ALTER TABLE milk_records ADD COLUMN deduction DECIMAL(10, 2) NOT NULL DEFAULT 0 AFTER base_rate;
-- ALTER TABLE milk_records ADD COLUMN final_rate DECIMAL(10, 2) NOT NULL DEFAULT 0 AFTER deduction;
