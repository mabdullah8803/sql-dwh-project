create or alter procedure bronze.load_bronze as 
begin
		print '===============================';
		print 'Loading Bronze Layer';
		print '===============================';
		
		print '-------------------------------';
		print 'Loading CRM Tables';
		print '-------------------------------';
		
		
		print 'Truncating Table: bronze.crm_cust_info';
		truncate table bronze.crm_cust_info;
		print 'Inserting Data into: bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'C:\Users\User\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			firstrow=2,
			fieldterminator = ',',
			tablock
		)
		
		
		print 'Truncating Table: bronze.bronze.crm_prod_info';
		truncate table bronze.crm_prod_info;
		print 'Inserting Data into: bronze.crm_prod_info';
		bulk insert bronze.crm_prod_info
		from 'C:\Users\User\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
			firstrow=2,
			fieldterminator = ',',
			tablock
		)
		
		print 'Truncating Table: bronze.crm_sales_details';
		truncate table bronze.crm_sales_details;
		print 'Inserting Data into: bronze.crm_sales_details';
		bulk insert bronze.crm_sales_details
		from 'C:\Users\User\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
			firstrow=2,
			fieldterminator = ',',
			tablock
		)
		
		print '-------------------------------';
		print 'Loading ERP Tables';
		print '-------------------------------';
		
		print 'Truncating Table: bronze.erp_cust_ez12';
		truncate table bronze.erp_cust_ez12;
		print 'Inserting Data into: bronze.erp_cust_ez12';
		bulk insert bronze.erp_cust_ez12
		from 'C:\Users\User\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
			firstrow=2,
			fieldterminator = ',',
			tablock
		)
		
		print 'Truncating Table: bronze.erp_loc_a101';
		truncate table bronze.erp_loc_a101;
		print 'Inserting Data into: bronze.erp_loc_a101';
		bulk insert bronze.erp_loc_a101
		from 'C:\Users\User\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
			firstrow=2,
			fieldterminator = ',',
			tablock
		)
		
		print 'Truncating Table: bronze.erp_px_cat_g1v2';
		truncate table bronze.erp_px_cat_g1v2;
		print 'Inserting Data into: bronze.erp_px_cat_g1v2';
		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\User\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
			firstrow=2,
			fieldterminator = ',',
			tablock
		)
end;
go

exec bronze.load_bronze
