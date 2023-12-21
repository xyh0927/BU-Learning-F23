CREATE PROCEDURE UpdateProductInfo
	@Product_id NVARCHAR(4000),
	@SKU NVARCHAR(4000),
	@Description NVARCHAR(4000),
	@Category NVARCHAR(4000)
AS BEGIN
	BEGIN TRANSACTION
		IF EXISTS (SELECT Product_id FROM Product WHERE Product_id = @Product_id) BEGIN
			IF (SELECT SKU FROM Product WHERE Product_id = @Product_id) != @SKU
				UPDATE Product Set SKU = @SKU WHERE Product_id = @Product_id;
			IF (SELECT Description FROM Product WHERE Product_id = @Product_id) != @Description
				UPDATE Product Set Description = @Description WHERE Product_id = @Product_id;
			IF (SELECT Description FROM Product WHERE Product_id = @Product_id) != @Description
				UPDATE Product Set Category = @Category WHERE Product_id = @Product_id;
		END;
		ELSE BEGIN
			INSERT INTO Product(Product_id,SKU,Description,Category)
			VALUES(@Product_id,@SKU,@Description,@Category)
		END;
	COMMIT;
END;

DROP PROCEDURE UpdateProductInfo;

EXEC UpdateProductInfo @Product_id = '574769', @SKU = 'GFE_19_USBLEDLight', @Description = 'Mobiles & Tablets',@Category = 'Mobiles & Tablets';
EXEC UpdateProductInfo @Product_id = '898989', @SKU = 'ROG_USBRGBMouse', @Description = 'ASUS Mouse',@Category = 'ELECTRONICS';

SELECT * FROM Product WHERE Product_id = '898989';