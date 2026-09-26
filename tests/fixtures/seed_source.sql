-- Data contoh untuk test end-to-end (dipakai di SQLite & Postgres).
-- Meniru gaya SQL Server: schema dbo, nama CamelCase, datetime tanpa timezone (jam WIB).
CREATE TABLE dbo."Customer" (
  "CustomerID" INTEGER PRIMARY KEY,
  "CustomerName" VARCHAR(100) NOT NULL,
  "Phone" VARCHAR(30),
  "City" VARCHAR(50),
  "CreatedDate" TIMESTAMP NOT NULL,
  "ModifiedDate" TIMESTAMP NOT NULL
);
INSERT INTO dbo."Customer" VALUES
 (1, 'Budi Santoso', '0811', 'semarang', '2026-09-01 08:00:00', '2026-09-20 09:15:00'),
 (2, 'Siti Aminah', NULL, 'JAKARTA', '2026-09-02 10:00:00', '2026-09-21 13:00:00'),
 (3, 'Andi Wijaya', '0813', 'depok', '2026-09-03 11:00:00', '2026-09-22 16:45:00'),
 (4, 'Dewi Lestari', '', 'bandung', '2026-09-04 12:00:00', '2026-09-23 07:30:00');

CREATE TABLE dbo."Product" (
  "ProductID" INTEGER PRIMARY KEY,
  "ProductCode" VARCHAR(20) NOT NULL,
  "ProductName" VARCHAR(100) NOT NULL,
  "Category" VARCHAR(50),
  "Price" NUMERIC(19, 4) NOT NULL,
  "IsActive" BOOLEAN
);
INSERT INTO dbo."Product" VALUES
 (10, 'ban-001', 'Ban Tubeless 80/90-17', 'BAN', 250000.0000, TRUE),
 (11, 'ban-002', 'Ban Tubeless 90/80-14', 'BAN', 275000.0000, TRUE),
 (12, 'oli-001', 'Oli Mesin 1L', 'OLI', 65000.0000, TRUE),
 (13, 'aki-001', 'Aki Kering', NULL, 350000.0000, FALSE);

CREATE TABLE dbo."SalesOrderHeader" (
  "SalesOrderID" INTEGER PRIMARY KEY,
  "OrderNumber" VARCHAR(30) NOT NULL,
  "OrderDate" TIMESTAMP NOT NULL,
  "CustomerID" INTEGER,
  "StoreCode" VARCHAR(10),
  "Status" VARCHAR(20),
  "TotalAmount" NUMERIC(19, 4),
  "ModifiedDate" TIMESTAMP NOT NULL
);
INSERT INTO dbo."SalesOrderHeader" VALUES
 (100, 'SO-100', '2026-09-20 10:00:00', 1, 'smg01', 'paid', 500000.0000, '2026-09-20 10:05:00'),
 (101, 'SO-101', '2026-09-21 11:00:00', 2, 'jkt01', 'paid', 65000.0000, '2026-09-21 11:05:00'),
 (102, 'SO-102', '2026-09-22 23:30:00', 3, 'smg01', 'cancelled', 275000.0000, '2026-09-22 23:35:00');

CREATE TABLE dbo."SalesOrderDetail" (
  "SalesOrderID" INTEGER NOT NULL,
  "SalesOrderDetailID" INTEGER NOT NULL,
  "ProductID" INTEGER NOT NULL,
  "Qty" NUMERIC(10, 2) NOT NULL,
  "UnitPrice" NUMERIC(19, 4) NOT NULL,
  "Discount" NUMERIC(19, 4),
  "LineTotal" NUMERIC(19, 4) NOT NULL,
  "ModifiedDate" TIMESTAMP NOT NULL,
  PRIMARY KEY ("SalesOrderID", "SalesOrderDetailID")
);
INSERT INTO dbo."SalesOrderDetail" VALUES
 (100, 1, 10, 2, 250000.0000, 0, 500000.0000, '2026-09-20 10:05:00'),
 (101, 1, 12, 1, 65000.0000, NULL, 65000.0000, '2026-09-21 11:05:00'),
 (102, 1, 11, 1, 275000.0000, 0, 275000.0000, '2026-09-22 23:35:00');
