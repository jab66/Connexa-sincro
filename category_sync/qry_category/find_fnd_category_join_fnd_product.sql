select  fc.id,
        fc.ext_code, 
        fc.name as desc_categoria, 
        fp.id as id_producto, 
        fp.description as desc_producto, 
        fp.ext_code as cod_prodcuto 
from fnd_category fc
inner join fnd_product fp on fp.category_id = fc.id 
where fc.id = %s  -- id de la categoria
