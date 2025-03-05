select 	ma.c_articulo as c_articulo, 
		ma.n_articulo as n_articulo, 
		precio_compra, 
		ean, 
		f_proc, 
		concat((select c_rubro_padre 
		        from t114_rubros tr 
				where c_rubro = ma.c_rubro), '-',
				ma.c_rubro,'-',
				ma.c_subrubro_1,'-', 
				ma.c_subrubro_2,	
				case 
					when ma.c_subrubro_3 = 999 then '' 
					else '-'||ma.c_subrubro_3 end 
				) rama,
		ta.m_baja 
from m_3_articulos ma
left join t050_articulos ta on ta.c_articulo = ma.c_articulo 


-- select 	c_articulo, 
-- 		n_articulo, 
-- 		precio_compra, 
-- 		ean, 
-- 		f_proc, 
-- 		concat((select c_rubro_padre 
-- 		        from t114_rubros tr 
-- 				where c_rubro = ma.c_rubro), '-',
-- 				c_rubro,'-',
-- 				c_subrubro_1,'-', 
-- 				c_subrubro_2,	
-- 				case 
-- 					when c_subrubro_3 = 999 then '' 
-- 					else '-'||c_subrubro_3 end 
-- 				) rama
-- from m_3_articulos ma

