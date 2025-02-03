select 	c_articulo, 
		n_articulo, 
		precio_compra, 
		ean, 
		f_proc, 
		concat((select c_rubro_padre 
		        from t114_rubros tr 
				where c_rubro = ma.c_rubro), '-',
				c_rubro,'-',
				c_subrubro_1,'-', 
				c_subrubro_2,	
				case 
					when c_subrubro_3 = 999 then '' 
					else '-'||c_subrubro_3 end 
				) rama
from m_3_articulos ma
-- where c_articulo = '6030'
-- where c_articulo in ('629', '552', '2652', '3231', '4240', '8876')
--where c_articulo in ('629', '552', '2652', '3231', '4240', '8876', '40', '100', '102')


-- select c_articulo, n_articulo, precio_compra, ean, f_proc, c_rubro, c_subrubro_1, c_subrubro_2, c_subrubro_3,
-- case when c_subrubro_3 = 999 and c_subrubro_2 != 999 then c_subrubro_2
-- 	 when c_subrubro_3 = 999 and c_subrubro_2 = 999 and c_subrubro_1 != 999 then c_subrubro_1
-- 	 when c_subrubro_3 = 999 and c_subrubro_2 = 999 and c_subrubro_1 = 999 and c_rubro != 999 then c_rubro
-- 	 else c_subrubro_3
-- end hoja
-- from m_3_articulos ma 
-- where c_articulo in ('629', '552', '2652', '3231', '4240', '8876')
-- order by c_articulo
-- limit 5000
