select c_articulo, n_articulo, coalesce(ean,'0') ean, COALESCE(ean1,'0') ean1, coalesce(ean2,'0') ean2, 
		coalesce(ean3,'0') ean3, coalesce(ean4,'0') ean4, coalesce(dun14,'0') dun14
from m_3_articulos ma --where c_articulo = '1313'
