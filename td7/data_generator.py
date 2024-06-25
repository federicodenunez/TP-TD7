import datetime
import random
from faker import Faker
from faker.providers import address, date_time, internet, passport, phone_number
import uuid

from td7.custom_types import Records

# innecesario
# PHONE_PROBABILITY = 0.7


class DataGenerator:
    def __init__(self):
        """Instantiates faker instance"""
        self.fake = Faker()
        self.fake.add_provider(address)
        self.fake.add_provider(date_time)
        self.fake.add_provider(internet)
        self.fake.add_provider(passport)
        self.fake.add_provider(phone_number)

    def generate_conductores(self, n: int):
        conductores = []
        for _ in range(n):
            conductor = {
                "apellido": self.fake.last_name(),
                'nombre': self.fake.first_name(),
                'nacionalidad': self.fake.country(),
                'fecha_nacimiento': self.fake.date_of_birth(minimum_age=18, maximum_age=50),
                'activo': "True"
            }
            conductores.append(conductor)
        return conductores

    def generate_equipos(self, n: int):
        equipos = []
        for _ in range(n):
            equipo = {
                'Nombre': self.fake.company(),
                'Pais': self.fake.country(),
                'Ano_creacion': random.randint(1800, datetime.datetime.now().year),
                'activo': "True"
            }
            equipos.append(equipo)
        return equipos

    def generate_sponsors(self, n: int):
        sponsors = []
        # TO DO: checkear que el nombre de sponsor no sea igual al de ningun equipo
        for _ in range(n):
            sponsor = {'Nombre': self.fake.company()}
            sponsors.append(sponsor)
        return sponsors

    def generate_sponsorea(self, n: int, sponsors, equipos):
        sponsorea = []
        for sponsor in sponsors:
            for equipo in random.sample(equipos, random.randint(1, len(equipos))):
                contrato = {
                    'Nombre_sponsor': sponsor['Nombre'],
                    'Nombre_equipo': equipo['Nombre'],
                    'Ano_contrato': random.randint(2000, datetime.datetime.now().year) # Checkear que no sea random
                }
                sponsorea.append(contrato)
        return sponsorea

    def generate_gp(self, n: int):
        gps = []
        for _ in range(n):
            gp = {
                'Nombre': self.fake.city(),
                'longitud': random.randint(3500, 7000),
                'pais': self.fake.country()
            }
            gps.append(gp)
        return gps

    def generate_una_edicion(self, GP, year:int, nro_carrera:int):
        # TO DO: hacer que sea una unica edicion
        # Tenemos que hacer que sea por un GP, con GP como parametro
        edicion = {
                    'Ano_carrera': year, # algo tenemos q meter para el año
                    'Nombre_carrera': GP['Nombre'],
                    'nro_carrera': nro_carrera,
                    'vuelta_record': random.randint(0, 300)
                }
        return edicion

    def generate_participan_en(equipos, edicion):
        participan_en = []
        for equipo in equipos:
                participation = {
                    'Nombre_equipo': equipo['Nombre'],
                    'Ano_carrera': edicion['Ano_carrera'],
                    'Nombre_carrera': edicion['Nombre_carrera']
                }
                participan_en.append(participation)
        return participan_en

    def generate_vehiculos(self, equipos):
        vehiculos = []
        for equipo in equipos:
                motor = self.fake.word()
                modelo = self.fake.word()
                for _ in range(2):
                    vehiculo = {
                        'Nombre_Equipo': equipo['Nombre'],
                        'Motor': motor,
                        'Modelo': modelo
                    }
                    vehiculos.append(vehiculo)
        return vehiculos

    # Esto se hace en el fill data, no se genera nada nuevo, solo se asignan cosas
    def generate_maneja_para(conductores, equipos, start_year, end_year):
        maneja_para = []
        for year in range(start_year, end_year + 1):
            for conductor in conductores:
                # MAL: equipo = random.choice(equipos)
                # Debería asignar conductores a equipos con plazas libres.
                manejo = {
                    'Id_conductor': conductor['id'],
                    'Nombre_equipo': equipos['Nombre'],
                    'ano': year
                }
                maneja_para.append(manejo)
        return maneja_para

    def generate_maneja_en(conductores, edicion, vehiculos):
        maneja_en = []
        shuffled_conductores = random.sample(conductores, len(conductores))  # Randomizo el orden de los conductores
        shuffled_positions_initial = random.sample(range(1, len(conductores) + 1), len(conductores))  # Randomizo posiciones iniciales
        shuffled_positions = list(range(1, 21))  # Randomizo posiciones finales
        for i, conductor in enumerate(shuffled_conductores):
            vehiculo = random.choice(vehiculos)
            carrera = {
                'ID_conductor': conductor['id'],
                'Ano_carrera': edicion['Ano_carrera'],
                'Nombre_carrera': edicion['Nombre_carrera'],
                'ID_Vehiculo': vehiculo['id'],
                'posicion_inicial': shuffled_positions_initial[i],
                'posicion': shuffled_positions[i]  
            }
        maneja_en.append(carrera)
        return maneja_en

    # TODO, mejorar mediciones.
    def generate_mediciones(vehiculos, edicion, climate:str):
        mediciones = []
        tipos = ['temp_motor', 'presion_ruedas', 'temp_cabina']
        if climate == "sunny":
                for vehiculo in vehiculos:
                    for _ in range(random.randint(500, 2500)):
                        medicion = {
                            'Id_Vehiculo': vehiculo['id'],
                            'Ano_carrera': edicion['Ano_carrera'],
                            'Nombre_carrera': edicion['Nombre_carrera'],
                            'tiempo_carrera': fake.date_time_this_year(), # aca tenemos
                            # que meter el tiempo desde 0 para hacer una time series
                            'tipo': random.choice(tipos),
                            'medicion': random.uniform(-100, 4000)
                        }
                        mediciones.append(medicion)
        if climate == "rainy":
            for vehiculo in vehiculos:
                    for _ in range(random.randint(500, 2500)):
                        medicion = {
                            'Id_Vehiculo': vehiculo['id'],
                            'Ano_carrera': edicion['Ano_carrera'],
                            'Nombre_carrera': edicion['Nombre_carrera'],
                            'tiempo_carrera': fake.date_time_this_year(), # aca tenemos
                            # que meter el tiempo desde 0 para hacer una time series
                            'tipo': random.choice(tipos),
                            'medicion': random.uniform(-100, 4000)
                        }
                        mediciones.append(medicion)
        if climate == "stormy":
            for vehiculo in vehiculos:
                    for _ in range(random.randint(500, 2500)):
                        medicion = {
                            'Id_Vehiculo': vehiculo['id'],
                            'Ano_carrera': edicion['Ano_carrera'],
                            'Nombre_carrera': edicion['Nombre_carrera'],
                            'tiempo_carrera': fake.date_time_this_year(), # aca tenemos
                            # que meter el tiempo desde 0 para hacer una time series
                            'tipo': random.choice(tipos),
                            'medicion': random.uniform(-100, 4000)
                        }
                        mediciones.append(medicion)
        return mediciones


