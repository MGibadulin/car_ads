IF (NOT EXISTS (SELECT * 
                 FROM INFORMATION_SCHEMA.TABLES 
                 WHERE TABLE_SCHEMA = 'dbo' 
                 AND  TABLE_NAME = 'tokenized_card'))
BEGIN

    CREATE TABLE car_ads_training_db.dbo.tokenized_card (
        ads_id	INT FOREIGN KEY,
        brand	VARCHAR(100) NOT NULL,
        model	VARCHAR(100) NOT NULL,
        price_primary	INT NOT NULL,
        price_history	VARCHAR(MAX),
        adress	VARCHAR(200),
        state	VARCHAR(50),
        zip_code	VARCHAR(100),
        vin_num	VARCHAR(100) NOT NULL,
        home_delivery_flag	CHAR(1) NOT NULL,
        virtual_appointments_flag	CHAR(1) NOT NULL,    
        comment	VARCHAR(MAX),
        year	INT NOT NULL,
        transmission_type	VARCHAR(50) NOT NULL,
        transmission_details	VARCHAR(200) NOT NULL,
        engine	INT NOT NULL,
        engine_details	VARCHAR(MAX) NOT NULL,
        fuel	VARCHAR(100) NOT NULL,
        mpg	VARCHAR(50) NOT NULL,
        mileage	INT NOT NULL,
        mileage_type	VARCHAR(50) NOT NULL,
        body	VARCHAR(100) NOT NULL,
        drive_type	VARCHAR(100) NOT NULL,
        color	VARCHAR(100) NOT NULL,
        vehicle_history	VARCHAR(MAX),
        scrap_date	DATETIME NOT NULL
    )

END