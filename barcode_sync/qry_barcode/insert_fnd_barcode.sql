INSERT INTO fnd_barcode(
        id, 
        barcode,
        timestamp,
        product_id,
        quantity,
        uom_id,
        barcode_type_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        
