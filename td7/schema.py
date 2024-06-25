from typing import Optional

from td7.custom_types import Records
from td7.database import Database

class Schema:
    def __init__(self):
        self.db = Database()        

    # Al poner el int opcional podemos elegir si solo queremos traer n registros de la tabla 
    # Lo dejo por si llega a ser util. 
    def get(self, table: str, sample_n: Optional[int] = None) -> Records:
        query = f"SELECT * FROM {table}"
        if sample_n is not None:
            query += f" LIMIT {sample_n}"
        
        return self.db.run_select(query)
    
              
    def get_conductores(self, sample_n: Optional[int] = None) -> Records:
        query = "SELECT * FROM Conductores WHERE activo = TRUE"
        if sample_n is not None:
            query += f" LIMIT {sample_n}"
        
        return self.db.run_select(query)
    
    def get_conductores(self, conductor_a_desactivar_id) -> Records:
        query = f"UPDATE Conductores SET activo = False WHERE Id_conductor is {conductor_a_desactivar_id}"
        return self.db.run_select(query)
    
    def get_carreras_actuales(self, year:int ):
        query = f"SELECT * FROM Ediciones WHERE {year} == Ano_carrera"
        return self.db.run_select(query)
    
    def get_sponsorea_last_year(self, year:int ):
        query = f"SELECT * FROM Sponsorea WHERE {year - 1} == Ano_contrato"
        return self.db.run_select(query)
    
    def get_maneja_para_last_year(self, n:int, year:int ):
        query = f"SELECT * FROM Maneja_para WHERE {year - 1} == Ano LIMIT {n}"
        return self.db.run_select(query)
    
    def get_equipos(self):
        query = f"SELECT * FROM Equipos WHERE activo = TRUE"
        return self.db.run_select(query)
    
    def get_ano(self) -> Records:
        query = f"SELECT max(Ano_contrato) FROM Sponsorea"
        return self.db.run_select(query)
    
    def get_vehiculos_last_year(self, sample_n: int = 20) -> Records:
        query = f"SELECT * FROM Vehiculos ORDER BY Id_Vehiculo DESC LIMIT {sample_n}"
        return self.db.run_select(query)


    def insert(self, records: Records, table: str):
        self.db.run_insert(records, table)

        """ Esto es el de base TP
          
    def get_people(self, sample_n: Optional[int] = None) -> Records:
        query = "SELECT * FROM people"
        if sample_n is not None:
            query += f" LIMIT {sample_n}"
        
        return self.db.run_select(query)

    def get_sessions(self) -> Records:
        return self.db.run_select("SELECT * FROM sessions")
        """