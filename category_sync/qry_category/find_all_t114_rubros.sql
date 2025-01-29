WITH RECURSIVE arbol_dependencia AS (
    -- Base de la recursión: empieza con los rubros raíz
    SELECT c_rubro, c_rubro_padre, d_rubro, 
           c_rubro::TEXT AS id_unico,  
           d_rubro::TEXT AS id_rubro,   
           1 AS nivel, f_alta, f_baja, m_baja
    FROM t114_rubros
    WHERE c_rubro_padre = 0
    UNION ALL  
    -- Parte recursiva: concatenar el c_rubro para id_unico y d_rubro para id_rubro
    SELECT t.c_rubro, t.c_rubro_padre, t.d_rubro, 
           arbol_dependencia.id_unico || '-' || t.c_rubro::TEXT AS id_unico, 
           arbol_dependencia.id_rubro || '-' || t.d_rubro::TEXT AS id_rubro,  
           arbol_dependencia.nivel + 1 AS nivel, t.f_alta, t.f_baja, t.m_baja
    FROM t114_rubros t
    JOIN arbol_dependencia ON t.c_rubro_padre = arbol_dependencia.c_rubro
)
-- Mostrar el árbol de dependencias con id_unico (con c_rubro) y id_rubro (con d_rubro)
SELECT c_rubro, nullif (c_rubro_padre,0) c_rubro_padre, 
d_rubro nombre_categoria, id_rubro ruta, id_unico, nivel, f_alta, f_baja, m_baja
FROM arbol_dependencia
where m_baja = 'N'  
--and split_part(id_unico, '-', 1) = '6'
--and c_rubro_padre = 6 or c_rubro = 6
--and  c_rubro_padre = 10 or c_rubro = 10
ORDER BY nivel, c_rubro_padre, c_rubro
