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

