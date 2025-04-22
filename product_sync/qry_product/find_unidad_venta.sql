select distinct
    case when loafum1 = 'Unidades' then 'unidad' else null end loafum1, loafcodigo 
from articulos_valkimia 
-- where loafcodigo = '1'

