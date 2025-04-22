class SupplierFunc:

    def __init__(self):
        # Conexiones a las bases de datos
        self.db_origen  = "dbname='diarco_data' user='postgres' password='aladelta10$' host='goldfarb.connexa-cloud.com'"
        self.db_destino = "dbname='connexa_platform' user='postgres' password='aladelta10$' host='goldfarb.connexa-cloud.com'"

        # Consulta para obtener datos desde la base de datos origen
        self.query_origen = """
            SELECT c_proveedor, c_cuit, n_proveedor, f_proc
            FROM public.m_10_proveedores 
        """

        # Consulta para obtener datos desde la base de datos destino (cargar el dataframe)
        self.query_destino = """
            SELECT ext_code FROM public.fnd_supplier
        """


    # Funcion para buscar id de proveedor en el dataframe de destino
    def search (self, dataframe, columna, valor):
        if dataframe.empty:
            return False
        else:
            campo = f"{columna}"
            dataframe = dataframe.set_axis([campo], axis=1)
            dataframe[campo] = dataframe[campo].astype('string')
            resultado = dataframe[dataframe[campo] == f"{valor}"]
            if resultado.empty:
                return False
            else:
                return True
        
