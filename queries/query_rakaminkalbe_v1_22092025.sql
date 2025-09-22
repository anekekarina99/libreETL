-- Buat tabel Inventory
CREATE TABLE IF NOT EXISTS Inventory (
    Item_code INTEGER PRIMARY KEY,
    Item_name TEXT,
    Item_price REAL,
    Item_total INTEGER
);

-- Insert data ke Inventory
INSERT OR IGNORE INTO Inventory (Item_code, Item_name, Item_price, Item_total)
VALUES
    (2341, 'Promag Tablet', 3000.0, 100),
    (2342, 'Hydro Coco 250ML', 7000.0, 20),
    (2343, 'Nutrive Benecol 100ML', 20000.0, 30),
    (2344, 'Blackmores Vit C 500Mg', 95000.0, 45),
    (2345, 'Entrasol Gold 370G', 90000.0, 120);

-- Tampilkan Item_name dengan Item_total tertinggi
SELECT Item_name
FROM Inventory
WHERE Item_total = (SELECT MAX(Item_total) FROM Inventory);

-- Update harga item dengan Item_total tertinggi
UPDATE Inventory
SET Item_price = 77500.0
WHERE Item_total = (SELECT MAX(Item_total) FROM Inventory);

-- Hapus item dengan Item_total terendah
DELETE FROM Inventory
WHERE Item_total = (SELECT MIN(Item_total) FROM Inventory);

-- Tampilkan semua data untuk verifikasi
SELECT * FROM Inventory;