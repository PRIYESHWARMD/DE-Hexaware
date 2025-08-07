CREATE DATABASE Car_Rental_System;
use Car_Rental_System;

CREATE TABLE vehicle (
    vehicle_id INT PRIMARY KEY IDENTITY(1,1),
    make VARCHAR(50),
    model VARCHAR(50),
    year INT,
    daily_rate DECIMAL(10, 2),
    status VARCHAR(20) CHECK (status IN ('available', 'notavailable')),
    passenger_capacity INT,
    engine_capacity DECIMAL(5, 2)
);

INSERT INTO vehicle (make, model, year, daily_rate, status, passenger_capacity, engine_capacity) VALUES
('Nissan', 'Altima', 2022, 32.00, 'available', 5, 2.5),
('Chevrolet', 'Cruze', 2021, 28.00, 'notavailable', 5, 1.8),
('BMW', 'X5', 2023, 70.00, 'available', 5, 3.0),
('Mercedes', 'C-Class', 2022, 75.00, 'available', 5, 2.0),
('Audi', 'A4', 2021, 68.00, 'notavailable', 5, 2.0),
('Kia', 'Seltos', 2020, 30.00, 'available', 5, 1.5),
('Volkswagen', 'Jetta', 2023, 34.00, 'available', 5, 1.4),
('Renault', 'Duster', 2021, 27.00, 'notavailable', 5, 1.6),
('Jeep', 'Compass', 2023, 50.00, 'available', 5, 2.0),
('Mazda', 'CX-5', 2022, 36.00, 'available', 5, 2.5),
('Skoda', 'Octavia', 2020, 33.00, 'available', 5, 1.8),
('Peugeot', '208', 2021, 26.00, 'available', 5, 1.2),
('Fiat', 'Punto', 2020, 22.00, 'available', 5, 1.3),
('Volvo', 'XC60', 2023, 65.00, 'available', 5, 2.0),
('Suzuki', 'Swift', 2022, 24.00, 'available', 5, 1.2),
('Toyota', 'Yaris', 2021, 27.00, 'available', 5, 1.3),
('Honda', 'Accord', 2023, 45.00, 'available', 5, 2.0),
('Ford', 'Focus', 2020, 29.00, 'available', 5, 1.6),
('Hyundai', 'Elantra', 2022, 33.00, 'available', 5, 1.8),
('MG', 'Hector', 2023, 50.00, 'available', 5, 2.0);

CREATE TABLE customer (
    customer_id INT PRIMARY KEY IDENTITY(1,1),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100) UNIQUE,
    phone_number VARCHAR(20)
);

INSERT INTO customer (first_name, last_name, email, phone_number) VALUES
('Alice', 'Smith', 'alice@example.com', '1234567890'),
('Bob', 'Johnson', 'bob@example.com', '0987654321'),
('Charlie', 'Brown', 'charlie@example.com', '1112223333'),
('Diana', 'Prince', 'diana@example.com', '4445556666'),
('Evan', 'Williams', 'evan@example.com', '7778889999'),
('Fiona', 'Garcia', 'fiona@example.com', '1111111111'),
('George', 'Martin', 'george@example.com', '2222222222'),
('Hannah', 'Lee', 'hannah@example.com', '3333333333'),
('Ian', 'Wright', 'ian@example.com', '4444444444'),
('Jane', 'Doe', 'jane@example.com', '5555555555'),
('Kyle', 'Young', 'kyle@example.com', '6666666666'),
('Laura', 'Green', 'laura@example.com', '7777777777'),
('Mike', 'Taylor', 'mike@example.com', '8888888888'),
('Nina', 'Patel', 'nina@example.com', '9999999999'),
('Oscar', 'Clark', 'oscar@example.com', '1010101010'),
('Paula', 'Hill', 'paula@example.com', '1212121212'),
('Quentin', 'Wood', 'quentin@example.com', '1313131313'),
('Rachel', 'King', 'rachel@example.com', '1414141414'),
('Steve', 'Adams', 'steve@example.com', '1515151515'),
('Tina', 'Lopez', 'tina@example.com', '1616161616');

CREATE TABLE lease (
    lease_id INT PRIMARY KEY IDENTITY(1,1),
    vehicle_id INT,
    customer_id INT,
    start_date DATE,
    end_date DATE,
    type VARCHAR(10) CHECK (type IN ('daily', 'monthly')),
    FOREIGN KEY (vehicle_id) REFERENCES vehicle(vehicle_id),
    FOREIGN KEY (customer_id) REFERENCES customer(customer_id)
);

INSERT INTO lease (vehicle_id, customer_id, start_date, end_date, type) VALUES
(1, 1, '2025-06-01', '2025-06-05', 'daily'),
(2, 2, '2025-06-10', '2025-07-10', 'monthly'),
(3, 3, '2025-06-15', '2025-06-20', 'daily'),
(4, 4, '2025-06-05', '2025-07-05', 'monthly'),
(5, 5, '2025-06-25', '2025-06-30', 'daily'),
(6, 6, '2025-07-01', '2025-07-10', 'daily'),
(7, 7, '2025-07-01', '2025-08-01', 'monthly'),
(8, 8, '2025-07-02', '2025-07-07', 'daily'),
(9, 9, '2025-07-03', '2025-08-03', 'monthly'),
(10, 10, '2025-07-04', '2025-07-09', 'daily'),
(11, 11, '2025-07-05', '2025-08-05', 'monthly'),
(12, 12, '2025-07-06', '2025-07-11', 'daily'),
(13, 13, '2025-07-07', '2025-08-07', 'monthly'),
(14, 14, '2025-07-08', '2025-07-13', 'daily'),
(15, 15, '2025-07-09', '2025-08-09', 'monthly'),
(16, 16, '2025-07-10', '2025-07-15', 'daily'),
(17, 17, '2025-07-11', '2025-08-11', 'monthly'),
(18, 18, '2025-07-12', '2025-07-17', 'daily'),
(19, 19, '2025-07-13', '2025-08-13', 'monthly'),
(20, 20, '2025-07-14', '2025-07-19', 'daily');

CREATE TABLE payment (
    payment_id INT PRIMARY KEY IDENTITY(1,1),
    lease_id INT,
    payment_date DATE,
    amount DECIMAL(10, 2),
    FOREIGN KEY (lease_id) REFERENCES lease(lease_id)
);

INSERT INTO payment (lease_id, payment_date, amount) VALUES
(1, '2025-06-01', 100.00),
(2, '2025-06-10', 1050.00),
(3, '2025-06-15', 200.00),
(4, '2025-06-05', 1500.00),
(5, '2025-06-25', 125.00),
(6, '2025-07-01', 180.00),
(7, '2025-07-01', 1020.00),
(8, '2025-07-02', 150.00),
(9, '2025-07-03', 1100.00),
(10, '2025-07-04', 160.00),
(11, '2025-07-05', 1300.00),
(12, '2025-07-06', 170.00),
(13, '2025-07-07', 1250.00),
(14, '2025-07-08', 175.00),
(15, '2025-07-09', 1400.00),
(16, '2025-07-10', 185.00),
(17, '2025-07-11', 1350.00),
(18, '2025-07-12', 190.00),
(19, '2025-07-13', 1450.00),
(20, '2025-07-14', 195.00);
-- average payment per vehicle
select v.vehicle_id, v.make + ' ' + v.model as vehicle_name,avg(p.amount) as avg_payment
from vehicle v join lease l on v.vehicle_id = l.vehicle_id join payment p on l.lease_id = p.lease_id
group by v.vehicle_id, v.make, v.model;

-- total leases per customer
select c.customer_id,c.first_name + ' ' + c.last_name as customer_name, count(l.lease_id) as total_leases
from customer c join lease l on c.customer_id = l.customer_id
group by c.customer_id, c.first_name, c.last_name;

-- total amount paid per lease
select l.lease_id,l.type,sum(p.amount) as total_paid
from lease l join payment p on l.lease_id = p.lease_id
group by l.lease_id, l.type;

-- max, min payment grouped by lease type
select l.type,max(p.amount) as max_payment,min(p.amount) as min_payment   
from payment p join lease l on p.lease_id = l.lease_id
group by l.type;


-- avg payment grouped by lease type
select l.type,avg(p.amount) as avg_payment  
from payment p join lease l on p.lease_id = l.lease_id
group by l.type;
