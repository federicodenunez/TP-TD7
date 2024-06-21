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
        query = "SELECT * FROM Conductores WHERE activo is TRUE"
        if sample_n is not None:
            query += f" LIMIT {sample_n}"
        
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