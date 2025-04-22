select 	loafcodigo as codigo_articulo, 
        loafum1 as unidad_medida,
		loafbarcode as barcode,
        LENGTH(loafbarcode) as longitud_barcode,
		loafufp as unidades_x_bulto,
		case when LENGTH(loafbarcode) = 14 then 5
		     when LENGTH(loafbarcode) = 13 then 1
		     when LENGTH(loafbarcode) = 8 then 11
		     when LENGTH(loafbarcode) = 12 then 2
		     else 0
		end as id_barcode,
		case when loafum1 = 'Unidades' then 'unidad' 
		end as id_um
from articulos_valkimia
group by loafcodigo, loafum1, articulos_valkimia.loafbarcode , loafufp



