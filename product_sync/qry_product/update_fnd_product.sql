UPDATE fnd_product
SET base_price = %s,
    description = %s,
    "timestamp" = %s,
    status_id = %s,
    category_id = %s
WHERE ext_code = %s

