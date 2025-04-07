--Tanaka (03-01-2025)
--ZAGIMORE DATABASE ETL CCODES

-- Customer Dimension
CREATE TABLE Customer_Dimension
(
  CustomerKey INT NOT NULL,
  CustomerID CHAR(8) NOT NULL,
  CustomerName VARCHAR(15) NOT NULL,
  CustomerZipo CHAR(8) NOT NULL,
  PRIMARY KEY (CustomerKey)
);

-- Store Dimension
CREATE TABLE Store_Dimension
(
  Store_Key INT NOT NULL,
  StoreZip CHAR(5) NOT NULL,
  RegionID CHAR(1) NOT NULL,
  RegionalName VARCHAR(25) NOT NULL,
  StoreID VARCHAR(3) NOT NULL,
  PRIMARY KEY (Store_Key)
);

--- Calendar Dimension
CREATE TABLE Calendar_Dimension
(
  Calendar_Key INT NOT NULL,
  FullDate DATE NOT NULL,
  MonthYear INT NOT NULL,
  Year INT NOT NULL,
  PRIMARY KEY (Calendar_Key)
);

-- Product Dimension
CREATE TABLE Product_Dimension
(
  ProductKey INT NOT NULL,
  Productname VARCHAR(25) NOT NULL,
  VendorId CHAR(2) NOT NULL,
  Vendorname VARCHAR(25) NOT NULL,
  Categoryname VARCHAR(25) NOT NULL,
  categoryID CHAR(2) NOT NULL,
  ProductSalesPrice Decimal(7,2),
  ProductDailyRentalPrice Decimal(7,2),
  ProductWeeklyRental Decimal(7,2),
  ProductType VARCHAR(10) NOT NULL,
  ProductId Char(3) NOT NULL,
  PRIMARY KEY (ProductKey)
);

--- Revenue Fcat Table 
CREATE TABLE RevenueFact
(
  RevenueGenerated INT NOT NULL,
  UnitsSold INT NOT NULL,
  TransactionID VARCHAR(8) NOT NULL,
  RevenueType VARCHAR(20) NOT NULL,
  CustomerKey INT NOT NULL,
  Store_Key INT NOT NULL,
  Calendar_Key INT NOT NULL,
  Product_Key INT NOT NULL,
  PRIMARY KEY (TransactionID, RevenueType, CustomerKey, Store_Key, Calendar_Key, Product_Key),
    FOREIGN KEY (ProductKey) REFERENCES Product_Dimension(Product_Key),
  FOREIGN KEY (CustomerKey) REFERENCES Customer_Dimension(CustomerKey),
  FOREIGN KEY (StoreKey) REFERENCES Store_Dimension(Store_Key),
  FOREIGN KEY (CalendarKey) REFERENCES Calendar_Dimension(Calendar_Key)
  );

--Data Staging tables creation code

--- Calendar Dimension
CREATE TABLE Calendar_Dimension
(
  Calendar_Key INT AUTO_INCREMENT,
  FullDate DATE NOT NULL,
  CalendarMonth INT,
  CalendarYear INT,
  PRIMARY KEY (Calendar_Key)
);

-- Product Dimension
CREATE TABLE Product_Dimension
(
  Product_Key INT AUTO_INCREMENT,
  Productname VARCHAR(25) NOT NULL,
  VendorId CHAR(2) NOT NULL,
  Vendorname VARCHAR(25) NOT NULL, 
ALTER TABLE mudzimtb_ZAGIMORE_DW.Product_cat_Dimension 
ADD PRIMARY KEY(Product_cat_Key);

CREATE TABLE mudzimtb_ZAGIMORE_DW.One_Way_Revenue_Agg_By_Product_Cat AS
SELECT * FROM One_Way_Revenue_Agg_By_Product_Cat;

ALTER TABLE mudzimtb_ZAGIMORE_DW.One_Way_Revenue_Agg_By_Product_Cat 
ADD PRIMARY KEY(Calendar_Key, Customer_Key, Store_Key, Product_cat_Key);

  ---Run in DS
ALTER TABLE mudzimtb_ZAGIMORE_DW.One_Way_Revenue_Agg_By_Product_Cat 
ADD Foreign Key (Calendar_Key) REFERENCES 
mudzimtb_ZAGIMORE_DW.Calendar_Dimension(Calendar_Key);

ALTER TABLE mudzimtb_ZAGIMORE_DW.One_Way_Revenue_Agg_By_Product_Cat 
ADD Foreign Key(Customer_Key) REFERENCES 
mudzimtb_ZAGIMORE_DW.Customer_Dimension(Customer_Key),
ADD Foreign Key(Store_Key) REFERENCES 
mudzimtb_ZAGIMORE_DW.Store_Dimension(Store_Key), 
ADD Foreign Key(Product_cat_Key) REFERENCES 
mudzimtb_ZAGIMORE_DW.Product_cat_dimension(Product_cat_Key)

--Daily store snapshot

CREATE TABLE Daily_Store_Snapshot AS
SELECT SUM(r.UnitSolds) AS TotalUnitSold, SUM(r.RevenueGenerated) AS TotalRevenueGenerated, 
COUNT(DISTINCT r.TID) AS TotalNumberOfTransactions, AVG(r.RevenueGenerated) 
AS AverageRevenueGenerated, r.Calendar_Key, r.Store_Key
FROM RevenueFact AS r
GROUP BY r.Calendar_Key, r.Store_Key

ALTER TABLE Daily_Store_Snapshot 
MODIFY COLUMN AverageRevenueGenerated DECIMAL(10,2)


--- 
CREATE TABLE FootwearRevenue AS
SELECT SUM(r.RevenueGenerated) AS TotalFootwearRevenue, r.Calendar_Key, r.Store_Key
FROM RevenueFact AS r, Product_Dimension AS pd
WHERE pd.CategoryName = "Footwear" 
AND pd.Product_Key = r. Product_Key
GROUP BY r.Calendar_Key, r.Store_Key
ORDER BY r.Calendar_Key ASC

-- Adding a column TotalFootwear Revenue to the Daily snapshot Revenue
ALTER TABLE Daily_Store_Snapshot
ADD COLUMN TotalFootwearRevenue INT DEFAULT 0

--- Updating Total Footwear values

UPDATE Daily_Store_Snapshot ds, FootwearRevenue fw
SET ds.TotalFootwearRevenue = fw.TotalFootwearRevenue
WHERE ds.Calendar_Key = fw.Calendar_Key
AND ds.Store_Key = fw.Store_Key

-- Add Column to the Daily Store Snapshot 
ALTER TABLE Daily_Store_Snapshot
ADD COLUMN NumberOfHVTransaction INT DEFAULT 0

CREATE TABLE HVTransaction AS 
SELECT COUNT(DISTINCT r.TID) as HVTransactionCount, r.Calendar_Key, r.Store_Key
FROM RevenueFact r
WHERE r.RevenueGenerated > 100
GROUP BY r.Calendar_Key, r.Store_Key


ALTER TABLE Daily_Store_Snapshot
ADD COLUMN TotalLocalRevenue INT DEFAULT 0


SELECT SUM(r.RevenueGenerated) AS TotalFootwearRevenue, r.Calendar_Key, r.Store_Key
FROM RevenueFact AS r, Store_Dimension AS sd, Customer_Dimension AS cd
WHERE LEFT(sd.StoreZip,2) = LEFT(cd.CustomerZip,2)
AND sd.Store_Key = r.Store_Key
AND sd.Customer_Key = r.Customer_Key
GROUP BY r.Calendar_Key, r.Store_Key

-- Local table 
CREATE TABLE TotalLocalRevenue AS
SELECT SUM(r.RevenueGenerated) AS TotalLocalRevenue, r.Calendar_Key, r.Store_Key
FROM RevenueFact AS r, Store_Dimension AS sd, Customer_Dimension AS cd
WHERE LEFT(sd.StoreZip,2) = LEFT(cd.CustomerZip,2)
AND sd.Store_Key = r.Store_Key
AND cd.Customer_Key = r.Customer_Key
GROUP BY r.Calendar_Key, r.Store_Key

UPDATE Daily_Store_Snapshot ds, TotalLocalRevenue lr
SET ds.TotalLocalRevenue = lr.TotalLocalRevenue
WHERE ds.Calendar_Key = lr.Calendar_Key
AND ds.Store_Key = lr.Store_Key

DROP TABLE TotalLocalRevenue;
DROP TABLE FootwearRevenue;
DROP TABLE HVTransaction

CREATE TABLE mudzimtb_ZAGIMORE_DW.Daily_Store_Snapshot AS
SELECT*
FROM Daily_Store_Snapshot;

ALTER TABLE mudzimtb_ZAGIMORE_DW.Daily_Store_Snapshot
ADD PRIMARY KEY (Calendar_Key, Store_Key);

ALTER TABLE mudzimtb_ZAGIMORE_DW.Daily_Store_Snapshot
ADD FOREIGN KEY (Calendar_Key) REFERENCES mudzimtb_ZAGIMORE_DW.Calendar_Dimension(Calendar_Key),
ADD FOREIGN KEY (Store_Key) REFERENCES mudzimtb_ZAGIMORE_DW.Store_Dimension(Store_Key);

ALTER TABLE RevenueFact
ADD ExtractionTimestamp TIMESTAMP, ADD f_loaded BOOLEAN

UPDATE RevenueFact
SET ExtractionTimestamp = NOW()-INTERVAL 10 DAY

UPDATE RevenueFact
SET f_loaded= TRUE

--4 zagimore
INSERT INTO salestransaction
(`tid`, `customerid`, `storeid`, `tdate`) 
VALUES ('ABC', '1-2-333', 'S10', '2025-03-25');

INSERT INTO `soldvia`
(`productid`, `tid`, `noofitems`)
VALUES ('1X2', 'ABC', '2'), ('2X4', 'ABC', '5');


INSERT INTO `salestransaction` (`tid`, `customerid`, `storeid`, `tdate`)
VALUES ('CDE', '6-7-888', 'S4', '2025-03-26');

INSERT INTO `soldvia` (`productid`, `tid`, `noofitems`) 
VALUES ('1X3', 'CDE', '6');

INSERT INTO `soldvia` (`productid`, `tid`, `noofitems`) 
VALUES ('1X2', 'CDE', '3');


INSERT INTO `rentaltransaction` (`tid`, `customerid`, `storeid`, `tdate`) 
VALUES ('FGH', '3-4-555', 'S7', '2025-03-26');

INSERT INTO `rentvia` (`productid`, `tid`, `rentaltype`, `duration`) 
VALUES ('1X1', 'FGH', 'D', '5');
INSERT INTO `rentvia` (`productid`, `tid`, `rentaltype`, `duration`) 
VALUES ('2X2', 'FGH', 'W', '6');


SELECT sv.noofitems as UnitSolds , p.productprice * sv.noofitems as RevenueGenerated , 'Sales' as RevenueType , sv.tid as TID , 
p.productid as ProductId , st.customerid as CustomerId , st.storeid as StoreId , st.tdate as FullDate  
FROM mudzimtb_ZAGIMORE.product as p , mudzimtb_ZAGIMORE.soldvia sv,mudzimtb_ZAGIMORE.salestransaction as st
WHERE sv.productid = p.productid 
AND sv.tid = st.tid
AND st.tdate > (SELECT MAX(DATE(ExtractionTimestamp))
FROM RevenueFact)

---
DROP TABLE  IntermediateFactTable
CREATE TABLE IntermediateFactTable AS 
SELECT sv.noofitems as UnitSolds , p.productprice * sv.noofitems as RevenueGenerated , 'Sales' as RevenueType , sv.tid as TID , 
p.productid as ProductId , st.customerid as CustomerId , st.storeid as StoreId , st.tdate as FullDate  
FROM mudzimtb_ZAGIMORE.product as p , mudzimtb_ZAGIMORE.soldvia sv,mudzimtb_ZAGIMORE.salestransaction as st
WHERE sv.productid = p.productid 
AND sv.tid = st.tid
AND st.tdate > (SELECT MAX(DATE(ExtractionTimestamp))
FROM RevenueFact)


ALTER TABLE IntermediateFactTable
MODIFY RevenueType VARCHAR (25);

---DAILY RENTAL EXTRACT TABLE FOR WEEKLY RENTALS BUT JUMB IT AND FINISH THE CODE THEN COME BACK TO THIS  after step 6.

INSERT INTO IntermediateFactTable(UnitSolds, RevenueGenerated, RevenueType, TID, productId, customerId, storeId, fullDate)
SELECT 0, r.productpricedaily * rv.duration , "RentalDaily", rv.tid, r.productid, c.customerid, s.storeid, rt.tdate
FROM mudzimtb_ZAGIMORE.rentalProducts r, mudzimtb_ZAGIMORE.rentvia rv,
mudzimtb_ZAGIMORE.customer c, mudzimtb_ZAGIMORE.store s, mudzimtb_ZAGIMORE.rentaltransaction rt
WHERE rv.productid = r.productid
AND rv.tid=rt.tid
AND c.customerid=rt.customerid
AND s.storeid = rt.storeid
AND rv.rentaltype= 'D'
AND rt.tdate > (SELECT MAX(DATE(ExtractionTimestamp)) 
FROM RevenueFact);

INSERT INTO IntermediateFactTable (UnitSolds, RevenueGenerated, RevenueType, TID, productId, customerId, storeId, fullDate)
SELECT 0, r.productpriceweekly * rv.duration AS RevenueGenerated, "RentalWeekly" AS RevenueType, rv.tid
AS TID, r.productid AS produtId, c.customerid AS customerId, s.storeid AS storeId, rt.tdate AS fullDate
FROM mudzimtb_ZAGIMORE.rentalProducts r, mudzimtb_ZAGIMORE.rentvia rv,
mudzimtb_ZAGIMORE.customer c, mudzimtb_ZAGIMORE.store s, mudzimtb_ZAGIMORE.rentaltransaction rt
WHERE rv.productid = r.productid
AND rv.tid=rt.tid
AND c.customerid=rt.customerid
AND s.storeid = rt.storeid
AND rv.rentaltype= 'W'
AND rt.tdate > (SELECT MAX(DATE(ExtractionTimestamp)) 
FROM RevenueFact);

INSERT INTO RevenueFact (UnitSolds, RevenueGenerated,RevenueType, TID,
Customer_Key,Store_Key, Product_Key, Calendar_Key, ExtractionTimestamp, f_loaded )
SELECT i.UnitSolds , i.RevenueGenerated , i.RevenueType,
i.TID, cd.Customer_Key , sd.Store_Key , pd.Product_Key ,
cad.Calendar_Key,NOW(), FALSE
FROM IntermediateFactTable as i , Customer_Dimension as cd,
Store_Dimension as sd, Product_Dimension as pd, Calendar_Dimension as cad
WHERE i.CustomerId = cd.CustomerId
AND sd.StoreId = i.StoreId
AND pd.ProductId = i.ProductId
AND cad.FullDate = i.FullDate
AND LEFT(pd.ProductType, 1) = LEFT (i.RevenueType, 1);

 --- THEN NEW ORDER 
 insert into mudzimtb_ZAGIMORE_DW.RevenueFact(RevenueGenerated,	UnitSolds,	RevenueType,	TID,	Customer_Key,
	Store_Key,	Product_Key,	Calendar_Key )
select RevenueGenerated,	UnitSolds,	RevenueType,	TID,	Customer_Key,	Store_Key,	Product_Key,	Calendar_Key
 from RevenueFact
 WHERE f_loaded= 0;

 UPDATE RevenueFact
 SET f_loaded= TRUE
 WHERE f_loaded = FALSE;

--- 
--1
INSERT INTO `salestransaction` (`tid`, `customerid`, `storeid`, `tdate`)
VALUES ('BBC', '7-8-999', 'S4', '2025-03-27');

INSERT INTO `soldvia` (`productid`, `tid`, `noofitems`) 
VALUES ('1X3', 'BBC', '6');

INSERT INTO `soldvia` (`productid`, `tid`, `noofitems`) 
VALUES ('1X2', 'CDE', '3');


INSERT INTO `rentaltransaction` (`tid`, `customerid`, `storeid`, `tdate`) 
VALUES ('FGH', '3-4-555', 'S7', '2025-03-27');

INSERT INTO `rentvia` (`productid`, `tid`, `rentaltype`, `duration`) 
VALUES ('1X1', 'FGH', 'D', '5');
INSERT INTO `rentvia` (`productid`, `tid`, `rentaltype`, `duration`) 
VALUES ('2X2', 'FGH', 'W', '6');


---- Procedure 

CREATE PROCEDURE Daily_Refresh_Procedure
BEGIN

DROP TABLE  IntermediateFactTable;

CREATE TABLE IntermediateFactTable AS 
SELECT sv.noofitems as UnitSolds , p.productprice * sv.noofitems as RevenueGenerated , 'Sales' as RevenueType , sv.tid as TID , 
p.productid as ProductId , st.customerid as CustomerId , st.storeid as StoreId , st.tdate as FullDate  
FROM mudzimtb_ZAGIMORE.product as p , mudzimtb_ZAGIMORE.soldvia sv,mudzimtb_ZAGIMORE.salestransaction as st
WHERE sv.productid = p.productid 
AND sv.tid = st.tid
AND st.tdate > (SELECT MAX(DATE(ExtractionTimestamp))
FROM RevenueFact);


ALTER TABLE IntermediateFactTable
MODIFY RevenueType VARCHAR (25);

INSERT INTO IntermediateFactTable(UnitSolds, RevenueGenerated, RevenueType, TID, productId, customerId, storeId, fullDate)
SELECT 0, r.productpricedaily * rv.duration , "RentalDaily", rv.tid, r.productid, c.customerid, s.storeid, rt.tdate
FROM mudzimtb_ZAGIMORE.rentalProducts r, mudzimtb_ZAGIMORE.rentvia rv,
mudzimtb_ZAGIMORE.customer c, mudzimtb_ZAGIMORE.store s, mudzimtb_ZAGIMORE.rentaltransaction rt
WHERE rv.productid = r.productid
AND rv.tid=rt.tid
AND c.customerid=rt.customerid
AND s.storeid = rt.storeid
AND rv.rentaltype= 'D'
AND rt.tdate > (SELECT MAX(DATE(ExtractionTimestamp)) 
FROM RevenueFact);

INSERT INTO IntermediateFactTable (UnitSolds, RevenueGenerated, RevenueType, TID, productId, customerId, storeId, fullDate)
SELECT 0, r.productpriceweekly * rv.duration AS RevenueGenerated, "RentalWeekly" AS RevenueType, rv.tid
AS TID, r.productid AS produtId, c.customerid AS customerId, s.storeid AS storeId, rt.tdate AS fullDate
FROM mudzimtb_ZAGIMORE.rentalProducts r, mudzimtb_ZAGIMORE.rentvia rv,
mudzimtb_ZAGIMORE.customer c, mudzimtb_ZAGIMORE.store s, mudzimtb_ZAGIMORE.rentaltransaction rt
WHERE rv.productid = r.productid
AND rv.tid=rt.tid
AND c.customerid=rt.customerid
AND s.storeid = rt.storeid
AND rv.rentaltype= 'W'
AND rt.tdate > (SELECT MAX(DATE(ExtractionTimestamp)) 
FROM RevenueFact);

INSERT INTO RevenueFact (UnitSolds, RevenueGenerated,RevenueType, TID,
Customer_Key,Store_Key, Product_Key, Calendar_Key, ExtractionTimestamp, f_loaded )
SELECT i.UnitSolds , i.RevenueGenerated , i.RevenueType,
i.TID, cd.Customer_Key , sd.Store_Key , pd.Product_Key ,
cad.Calendar_Key,NOW(), FALSE
FROM IntermediateFactTable as i , Customer_Dimension as cd,
Store_Dimension as sd, Product_Dimension as pd, Calendar_Dimension as cad
WHERE i.CustomerId = cd.CustomerId
AND sd.StoreId = i.StoreId
AND pd.ProductId = i.ProductId
AND cad.FullDate = i.FullDate
AND LEFT(pd.ProductType, 1) = LEFT (i.RevenueType, 1);

 insert into mudzimtb_ZAGIMORE_DW.RevenueFact(RevenueGenerated,	UnitSolds,	RevenueType,	TID,	Customer_Key,
	Store_Key,	Product_Key,	Calendar_Key )
select RevenueGenerated,	UnitSolds,	RevenueType,	TID,	Customer_Key,	Store_Key,	Product_Key,	Calendar_Key
 from RevenueFact
 WHERE f_loaded= 0;

UPDATE RevenueFact
SET f_loaded = True
WHERE f_loaded = False;

END
---- Daily refresh of Product Dimension 

ALTER TABLE Product_Dimension
ADD ExtractionTimestamp TIMESTAMP,
ADD PDloaded BOOLEAN

UPDATE Product_Dimension
SET ExtractionTimestamp =NOW()- INTERVAL 20 DAY;

UPDATE Product_Dimension
SET PDLoaded=True;
-- run in ZAGIMORE   for
INSERT INTO `product` (`productid`, `productname`, `productprice`, `vendorid`, `categoryid`) VALUES ('1Z1', 'Bottle', '34', 'OA', 'CY');

-- Back to DS 
INSERT INTO Product_Dimension (ProductId, Productname, ProductSalesPrice, VendorId, Vendorname, categoryID, Categoryname, 
ProductType,ExtractionTimestamp, PDLoaded)
SELECT p.productid, p.productname , p.productprice, v.vendorid, v.vendorname ,c.categoryid, c.categoryname , 'Sales', NOW(), false
FROM  mudzimtb_ZAGIMORE.product as p , mudzimtb_ZAGIMORE.category as c, mudzimtb_ZAGIMORE.vendor as v 
WHERE c.categoryid = p.categoryid and p.vendorid = v.vendorid 
AND p.productId not in (SELECT ProductId FROM Product_Dimension WHERE producttype="Sales")


---Run in DS its loading to DW
INSERT INTO mudzimtb_ZAGIMORE_DW.Product_Dimension (Product_Key, ProductId, Productname, ProductSalesPrice, VendorId, Vendorname, categoryID, Categoryname, 
ProductType)
SELECT Product_Key, ProductId, Productname, ProductSalesPrice, VendorId, Vendorname, categoryID, Categoryname, ProductType
FROM Product_Dimension
WHERE PDLoaded = FALSE

UPDATE Product_Dimension
SET PDLoaded = True


--ZAGIMORE  RENTALProducts

INSERT INTO `rentalProducts` (`productid`, `productname`, `vendorid`, `categoryid`, `productpricedaily`, `productpriceweekly`) VALUES ('F1D', 'Ferrari', 'OA', 'EL', '50', '200');
INSERT INTO `rentalProducts`(`productid`, `productname`, `vendorid`, `categoryid`, `productpricedaily`, `productpriceweekly`) VALUES ('1X6','Birmingham','PG','EL','100','700');

INSERT INTO Product_Dimension ( Productname, VendorId, Vendorname,Categoryname, CategoryID,  ProductId,ProductType, ProductDailyRentalPrice, ProductWeeklyRental, ExtractionTimeStamp,PDloaded)
SELECT p.productname,v.vendorid, v.vendorname ,c.categoryname,c.categoryid,p.productid, 'Rental', p.productpricedaily, p.productpriceweekly,NOW(), FALSE
FROM  mudzimtb_ZAGIMORE.rentalProducts as p , mudzimtb_ZAGIMORE.category as c, mudzimtb_ZAGIMORE.vendor as v 
WHERE c.categoryid = p.categoryid and p.vendorid = v.vendorid 
AND p.productid NOT IN (SELECT productid FROM Product_Dimension WHERE producttype="Rental");

INSERT INTO mudzimtb_ZAGIMORE_DW.Product_Dimension( Product_Key,Productname, VendorId, Vendorname,Categoryname, CategoryID,  ProductId,ProductType, ProductDailyRentalPrice, ProductWeeklyRental)
SELECT Product_Key,Productname, VendorId, Vendorname,Categoryname, CategoryID,  ProductId,ProductType, ProductDailyRentalPrice, ProductWeeklyRental
FROM Product_Dimension
WHERE PDLoaded = FALSE

UPDATE Product_Dimension
SET PDLoaded = True

INSERT INTO `rentalProducts`(`productid`, `productname`, `vendorid`, `categoryid`, `productpricedaily`, `productpriceweekly`) VALUES ('1R8','Birmingham','PG','EL','100','700');
INSERT INTO `product` (`productid`, `productname`, `productprice`, `vendorid`, `categoryid`) VALUES ('1R1', 'Bottle', '34', 'OA', 'CY');
INSERT INTO `product` (`productid`, `productname`, `productprice`, `vendorid`, `categoryid`) VALUES ('2Z2', 'COFFEE', '10', 'OA', 'CY');
INSERT INTO `rentalProducts` (`productid`, `productname`, `vendorid`, `categoryid`, `productpricedaily`, `productpriceweekly`) VALUES ('2Z2', 'COFFEE', 'WL', 'CY', '20', '1000');

---Procedure dont forget to put the $$$ on spce boc when creating the procedure

CREATE PROCEDURE Daily_Product_Refresh()
BEGIN

INSERT INTO Product_Dimension (ProductId, Productname, ProductSalesPrice, VendorId, Vendorname, categoryID, Categoryname, 
ProductType,ExtractionTimestamp, PDLoaded)
SELECT p.productid, p.productname , p.productprice, v.vendorid, v.vendorname ,c.categoryid, c.categoryname , 'Sales', NOW(), FALSE
FROM  mudzimtb_ZAGIMORE.product as p , mudzimtb_ZAGIMORE.category as c, mudzimtb_ZAGIMORE.vendor as v 
WHERE c.categoryid = p.categoryid and p.vendorid = v.vendorid 
AND p.productId not in (SELECT ProductId FROM Product_Dimension WHERE producttype="Sales");


INSERT INTO Product_Dimension ( Productname, VendorId, Vendorname,Categoryname, CategoryID,  ProductId,ProductType, ProductDailyRentalPrice, ProductWeeklyRental, ExtractionTimeStamp,PDloaded)
SELECT p.productname,v.vendorid, v.vendorname ,c.categoryname,c.categoryid,p.productid, 'Rental', p.productpricedaily, p.productpriceweekly,NOW(), FALSE
FROM  mudzimtb_ZAGIMORE.rentalProducts as p , mudzimtb_ZAGIMORE.category as c, mudzimtb_ZAGIMORE.vendor as v 
WHERE c.categoryid = p.categoryid and p.vendorid = v.vendorid 
AND p.productid NOT IN (SELECT productid FROM Product_Dimension WHERE producttype="Rental");

INSERT INTO mudzimtb_ZAGIMORE_DW.Product_Dimension( Product_Key, Productname, VendorId, Vendorname,Categoryname, CategoryID,  ProductId,ProductType,ProductSalesPrice, 
ProductDailyRentalPrice, ProductWeeklyRental)
SELECT Product_Key, Productname, VendorId, Vendorname,Categoryname, CategoryID,  ProductId,ProductType,ProductSalesPrice, ProductDailyRentalPrice, ProductWeeklyRental
FROM Product_Dimension
WHERE PDLoaded =FALSE;

UPDATE Product_Dimension
SET PDLoaded = True;

END

-- Daily refresh for for STORE DIMENSION
ALTER TABLE Store_Dimension
ADD ExtractionTimeStamp TIMESTAMP, ADD Sloaded BOOLEAN;

UPDATE Store_Dimension
SET Sloaded = True;

UPDATE Store_Dimension
SET ExtractionTimeStamp = NOW() - INTERVAL 20 DAY;

INSERT INTO store(storeid, storezip, regionid) VALUES ('S15','13676','N')
INSERT INTO store(storeid, storezip, regionid) VALUES ('S16','13676','C')

INSERT INTO Store_Dimension(StoreID,StoreZip,RegionID, RegionName, ExtractionTimeStamp, Sloaded)
SELECT s.storeid, s.storezip, s.regionid, r.regionname, NOW(), FALSE
FROM mudzimtb_ZAGIMORE.store AS s, mudzimtb_ZAGIMORE.region AS r
WHERE s.regionid = r.regionid
AND s.storeID NOT IN (SELECT storeid FROM Store_Dimension);

INSERT INTO mudzimtb_ZAGIMORE_DW.Store_Dimension(Store_Key,StoreID,StoreZip,RegionID, RegionName)
SELECT Store_Key,StoreID,StoreZip,RegionID, RegionName
FROM Store_Dimension
WHERE Sloaded = FALSE;

UPDATE Store_Dimension
SET Sloaded=TRUE;

--store dimension refresh procedure

INSERT INTO store(storeid, storezip, regionid) VALUES ('S17','19245','T')
INSERT INTO store(storeid, storezip, regionid) VALUES ('S18','19245','I')

CREATE PROCEDURE Daily_Store_Refresh()
BEGIN

INSERT INTO Store_Dimension(StoreID,StoreZip,RegionID, RegionName, ExtractionTimeStamp, Sloaded)
SELECT s.storeid, s.storezip, s.regionid, r.regionname, NOW(), FALSE
FROM mudzimtb_ZAGIMORE.store AS s, mudzimtb_ZAGIMORE.region AS r
WHERE s.regionid = r.regionid
AND s.storeID NOT IN (SELECT storeid FROM Store_Dimension);

INSERT INTO mudzimtb_ZAGIMORE_DW.Store_Dimension(Store_Key,StoreID,StoreZip,RegionID, RegionName)
SELECT Store_Key,StoreID,StoreZip,RegionID, RegionName
FROM Store_Dimension
WHERE Sloaded = FALSE;

UPDATE Store_Dimension
SET Sloaded=TRUE;

END

-- Daily refresh for CUSTOMER DIMENSION
ALTER TABLE Customer_Dimension
ADD ExtractionTimeStamp TIMESTAMP, ADD Cloaded BOOLEAN;

UPDATE Customer_Dimension
SET Cloaded = True;

UPDATE Customer_Dimension
SET ExtractionTimeStamp = NOW() - INTERVAL 20 DAY;

INSERT INTO customer(customerid, customername, customerzip) VALUES ('3-7-999','Golden','13676');
INSERT INTO customer(customerid, customername, customerzip) VALUES ('7-3-666','Knight','13677');


INSERT INTO Customer_Dimension(CustomerID, CustomerName, CustomerZip,ExtractionTimeStamp, Cloaded)
SELECT c.customerid, c.customername, c.customerzip,NOW(), FALSE
FROM mudzimtb_ZAGIMORE.customer AS c
WHERE c.customerid NOT IN (SELECT customerID FROM mudzimtb_ZAGIMORE_DS.Customer_Dimension);

insert into mudzimtb_ZAGIMORE_DW.Customer_Dimension(Customer_Key,CustomerName,CustomerZip,CustomerId) 
select Customer_Key,CustomerName,CustomerZip,CustomerId 
FROM Customer_Dimension
WHERE Cloaded = FALSE;

UPDATE Customer_Dimension
SET Cloaded=TRUE;

--customer dimension refresh procedure

INSERT INTO customer(customerid, customername, customerzip) VALUES ('9-6-666','Connor','13676');
INSERT INTO customer(customerid, customername, customerzip) VALUES ('8-4-777','Bedard','13677');

---Procedure
CREATE PROCEDURE Daily_Customer_Refresh()
BEGIN

INSERT INTO Customer_Dimension(CustomerID, CustomerName, CustomerZip,ExtractionTimeStamp, Cloaded)
SELECT c.customerid, c.customername, c.customerzip,NOW(), FALSE
FROM mudzimtb_ZAGIMORE.customer AS c
WHERE c.customerid NOT IN (SELECT customerID FROM mudzimtb_ZAGIMORE_DS.Customer_Dimension);

insert into mudzimtb_ZAGIMORE_DW.Customer_Dimension(Customer_Key,CustomerName,CustomerZip,CustomerId) 
select Customer_Key,CustomerName,CustomerZip,CustomerId 
FROM Customer_Dimension
WHERE Cloaded = FALSE;

UPDATE Customer_Dimension
SET Cloaded=TRUE;   
END

-- CREATE A PROCEDURE FOR LATEFACTFRESH
CREATE PROCEDURE LateFactRefresh()
BEGIN 
DROP TABLE IF EXISTS IntermediateFactTable;
CREATE TABLE IntermediateFactTable AS 
SELECT sv.noofitems as UnitSolds , p.productprice * sv.noofitems as RevenueGenerated , 'Sales' as RevenueType , sv.tid as TID , 
p.productid as ProductId , st.customerid as CustomerId , st.storeid as StoreId , st.tdate as FullDate  
FROM mudzimtb_ZAGIMORE.product as p , mudzimtb_ZAGIMORE.soldvia sv,mudzimtb_ZAGIMORE.salestransaction as st
WHERE sv.productid = p.productid 
AND sv.tid = st.tid
AND st.tid NOT IN 
(SELECT TID FROM RevenueFact
WHERE RevenueType = 'Sales'
);

ALTER TABLE IntermediateFactTable
MODIFY RevenueType VARCHAR(25);

INSERT INTO IntermediateFactTable(UnitSolds, RevenueGenerated, RevenueType, TID, productId, customerId, storeId, fullDate)
SELECT 0, r.productpricedaily * rv.duration , "RentalDaily", rv.tid, r.productid, c.customerid, s.storeid, rt.tdate
FROM mudzimtb_ZAGIMORE.rentalProducts r, mudzimtb_ZAGIMORE.rentvia rv,
mudzimtb_ZAGIMORE.customer c, mudzimtb_ZAGIMORE.store s, mudzimtb_ZAGIMORE.rentaltransaction rt
WHERE rv.productid = r.productid
AND rv.tid=rt.tid
AND c.customerid=rt.customerid
AND s.storeid = rt.storeid
AND rv.rentaltype= 'D'
AND rt.tid NOT IN (
SELECT TID FROM RevenueFact
WHERE  RevenueType LIKE 'R%');

INSERT INTO IntermediateFactTable (UnitSolds, RevenueGenerated, RevenueType, TID, productId, customerId, storeId, fullDate)
SELECT 0, r.productpriceweekly * rv.duration AS RevenueGenerated, "RentalWeekly" AS RevenueType, rv.tid
AS TID, r.productid AS produtId, c.customerid AS customerId, s.storeid AS storeId, rt.tdate AS fullDate
FROM mudzimtb_ZAGIMORE.rentalProducts r, mudzimtb_ZAGIMORE.rentvia rv,
mudzimtb_ZAGIMORE.customer c, mudzimtb_ZAGIMORE.store s, mudzimtb_ZAGIMORE.rentaltransaction rt
WHERE rv.productid = r.productid
AND rv.tid=rt.tid
AND c.customerid=rt.customerid
AND s.storeid = rt.storeid
AND rv.rentaltype= 'W'
AND rt.tid NOT IN (
    SELECT TID FROM RevenueFact
    WHERE RevenueType LIKE 'R%'
);

INSERT INTO RevenueFact (UnitSolds, RevenueGenerated,RevenueType, TID,
Customer_Key,Store_Key, Product_Key, Calendar_Key, ExtractionTimestamp, f_loaded )
SELECT i.UnitSolds , i.RevenueGenerated , i.RevenueType,
i.TID, cd.Customer_Key , sd.Store_Key , pd.Product_Key ,
cad.Calendar_Key,NOW(), FALSE
FROM IntermediateFactTable as i , Customer_Dimension as cd,
Store_Dimension as sd, Product_Dimension as pd, Calendar_Dimension as cad
WHERE i.CustomerId = cd.CustomerId
AND sd.StoreId = i.StoreId
AND pd.ProductId = i.ProductId
AND cad.FullDate = i.FullDate
AND LEFT(pd.ProductType, 1) = LEFT (i.RevenueType, 1);

INSERT INTO mudzimtb_ZAGIMORE_DW.RevenueFact(RevenueGenerated,	UnitSolds,	RevenueType,	TID,	Customer_Key,
	Store_Key,	Product_Key,	Calendar_Key )
SELECT RevenueGenerated,	UnitSolds,	RevenueType,	TID,	Customer_Key,	Store_Key,	Product_Key,	Calendar_Key
 FROM RevenueFact
 WHERE f_loaded= 0;

 UPDATE RevenueFact
 SET f_loaded= TRUE
 WHERE f_loaded = 0;

 END

-- run these to test the procedure created above
INSERT INTO `salestransaction` (`tid`, `customerid`, `storeid`, `tdate`)
VALUES ('NEWST', '6-7-888', 'S4', '2025-03-26');

INSERT INTO `soldvia` (`productid`, `tid`, `noofitems`)
VALUES ('1X3', 'NEWST', '6');

INSERT INTO `rentaltransaction` (`tid`, `customerid`, `storeid`, `tdate`)
VALUES ('NEWRT', '3-4-555', 'S7', '2025-03-26');

INSERT INTO `rentvia` (`productid`, `tid`, `rentaltype`, `duration`)
VALUES ('1X1', 'NEWRT', 'D', '5');
INSERT INTO `rentvia` (`productid`, `tid`, `rentaltype`, `duration`)
VALUES ('2X2', 'NEWRT', 'W', '6');.












