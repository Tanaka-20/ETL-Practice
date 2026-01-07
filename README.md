
# ZAGIMORE Data Warehouse ETL (MySQL)

This repository contains SQL scripts used to design and refresh the **ZAGIMORE Data Warehouse (DW)** using an **ETL pipeline** from the operational **ZAGIMORE** database into a star-schema DW.

Author: **Tanaka**  
Date: **03-01-2025**

---

## Project Overview

The goal of this project is to build a star-schema **data warehouse** that supports analytics such as:

- Revenue and units sold across time, stores, customers, and products
- Category-level revenue aggregation (e.g., Footwear)
- Daily store performance snapshots (KPIs per store per date)
- Incremental fact loading using extraction timestamps and load flags
- Refresh procedures to support daily ETL automation
- Handling **late arriving facts**
- Handling **SCD Type 2** changes for dimensions (DVF / DVU / CurrentStatus)

---

## Architecture

### Source System (OLTP)
- `mudzimtb_ZAGIMORE` (sales + rentals transactions)

### Staging / Data Store (DS)
- `mudzimtb_ZAGIMORE_DS` (intermediate ETL tables and load flags)

### Data Warehouse (DW)
- `mudzimtb_ZAGIMORE_DW` (star schema + analytics tables)

---

## Star Schema Design

### Dimensions
- **Customer_Dimension**
  - CustomerKey, CustomerID, CustomerName, CustomerZip  
- **Store_Dimension**
  - Store_Key, StoreID, StoreZip, RegionID, RegionName  
- **Calendar_Dimension**
  - Calendar_Key, FullDate, CalendarMonth, CalendarYear  
- **Product_Dimension**
  - Product_Key, ProductId, Productname, VendorId, Vendorname, CategoryId, Categoryname  
  - Prices for sales + rentals (daily / weekly), ProductType

### Fact Table
- **RevenueFact**
  - Measures: `RevenueGenerated`, `UnitSolds`
  - Keys: Customer_Key, Store_Key, Product_Key, Calendar_Key
  - Supports multiple revenue types: `Sales`, `RentalDaily`, `RentalWeekly`

---

## ETL Workflow

### 1) Dimension Loading (DS → DW)
Dimensions are loaded from the source system into DS, then pushed into DW.

Each dimension supports incremental loads using:
- `ExtractionTimestamp`
- Loaded flag (`Cloaded`, `Sloaded`, `PDloaded`)
- Optional SCD Type 2 fields:
  - `DVF` (Date Valid From)
  - `DVU` (Date Valid Until)
  - `CurrentStatus` (C = current, N = not current)

**Refresh Procedures**
- `Daily_Product_Refresh()`
- `Daily_Store_Refresh()`
- `Daily_Customer_Refresh()`

---

### 2) Fact Loading (Incremental Fact ETL)
Facts are extracted into an **IntermediateFactTable**, then loaded into `RevenueFact`.

#### Extract Sources
- Sales: `salestransaction + soldvia + product`
- Rentals:
  - Daily rentals: `rentaltransaction + rentvia + rentalProducts`
  - Weekly rentals: `rentaltransaction + rentvia + rentalProducts`

#### Incremental Filter
New facts are extracted using:
- `tdate > MAX(DATE(ExtractionTimestamp))` from `RevenueFact`

#### Fact Load Flags
Fact table tracks ETL status using:
- `ExtractionTimestamp`
- `f_loaded` (boolean)

---

### 3) Daily Refresh Procedure (Facts)
A full ETL refresh for new daily facts is handled by:

- `Daily_Refresh_Procedure`

This procedure:
1. Rebuilds `IntermediateFactTable`
2. Inserts sales + rental facts
3. Loads facts into DS `RevenueFact`
4. Pushes only new rows into DW `RevenueFact`
5. Updates `f_loaded` to prevent duplicates

---

## Analytics Tables (DW Layer)

### Daily Store Snapshot
Creates daily KPIs per store:

- Total units sold
- Total revenue generated
- Number of transactions
- Average revenue per transaction
- Total footwear revenue
- Total local revenue (store zip prefix matches customer zip prefix)

Tables involved:
- `Daily_Store_Snapshot`
- Temporary helper tables:
  - `FootwearRevenue`
  - `TotalLocalRevenue`
  - `HVTransaction` (high value transactions > 100)

---

## Late Arriving Facts

Some transactions may arrive late (missing from previous loads).  
The project includes a stored procedure to recover them:

- `LateFactRefresh()`

This procedure extracts facts where:
- `TID NOT IN RevenueFact`
and then loads them into both DS and DW safely.

---

## SCD Type 2 Handling (Dimensions)

This ETL supports SCD Type 2 changes primarily for **Product_Dimension** (and also applied to Store/Customer fields).

Key fields:
- `DVF` = Date Valid From  
- `DVU` = Date Valid Until  
- `CurrentStatus` = `C` (current) or `N` (expired)

Example logic:
1. Detect changed products (price/name/vendor changes)
2. Expire old row:
   - `DVU = NOW() - 1 day`
   - `CurrentStatus = 'N'`
3. Insert a new current row:
   - `DVF = NOW()`
   - `DVU = '2040-01-01'`
   - `CurrentStatus = 'C'`
4. `REPLACE INTO` DW dimension to sync

---

## How To Use This Repo

### Requirements
- MySQL / MariaDB
- Access to:
  - `mudzimtb_ZAGIMORE` (source OLTP)
  - `mudzimtb_ZAGIMORE_DS` (staging)
  - `mudzimtb_ZAGIMORE_DW` (data warehouse)

### Suggested Execution Order
1. Create dimensions in DS and DW
2. Create fact table in DS and DW
3. Run initial full load scripts
4. Create refresh stored procedures:
   - `Daily_Refresh_Procedure`
   - `Daily_Product_Refresh`
   - `Daily_Store_Refresh`
   - `Daily_Customer_Refresh`
   - `LateFactRefresh`
5. Run daily refresh procedures for incremental ETL

---

## Repository Contents (Suggested Structure)

- `/sql/schema/`  
  Star schema tables (dimensions + fact)

- `/sql/procedures/`  
  Stored procedures for daily refresh + late facts

- `/sql/analytics/`  
  Snapshot tables and revenue aggregation scripts

---

## Notes / Common Issues

- Some scripts require `DROP TABLE IF EXISTS` before creating temp tables.
- When creating procedures in MySQL Workbench, ensure you use delimiter syntax:
  - `DELIMITER $$` ... `$$` ... `DELIMITER ;`
- Confirm foreign key column names match exactly (e.g., `Product_Key` vs `ProductKey`).

---

## Author
**Tanaka Mudzimbasekwa**  
Master’s Student — Applied Data Science  
ETL / Data Warehousing / SQL Analytics
