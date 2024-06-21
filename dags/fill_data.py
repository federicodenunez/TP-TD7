from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
import datetime
import random
from td7.data_generator import DataGenerator
from td7.schema import Schema

EVENTS_PER_DAY = 10_000

def generate_data_annualy(base_time: str):
    # Inicializo al generador y a mi schema
    generator = DataGenerator()
    schema = Schema()

    # Conductores de la temporada actual
    # Elimino a (1-3) conductores actuales y creo 3 nuevos
    conductores_actuales = schema.get_conductores(20)
    conductores_a_borrar = random.randint(1,3)
    for i in range(conductores_a_borrar):
        random_conductor = random.randint(0,len(conductores_actuales))
        del conductores_actuales[random_conductor]
        # TODO DESACTIVARLOS

    # Reemplazo a esos conductores
    nuevos_conductores = generator.generate_conductores(conductores_a_borrar)
    schema.insert(nuevos_conductores, "conductores")
    # Entonces tenemos 20 conductores con activo
    # Esto me sirve para después porque se relaciona con otros equipos.
    conductores_actuales = conductores_actuales + nuevos_conductores


    # Equipos de la temporada actual
    equipos_actuales = schema.get("equipos", 10)
    equipos_a_borrar = random.randint(0,2)
    # tengo que borrar a sus sponsors primero
    for i in range(equipos_a_borrar):
        random_equipo = random.randint(0,len(equipos_actuales))
        del equipos_actuales(random_equipo)
    # Creo nuevos equipos
    nuevos_equipos = generator.generate_equipos(equipos_a_borrar)
    # Lo añado a los equipos a devolver.
    nuevos_equipos = equipos_actuales + nuevos_equipos
    schema.insert(nuevos_equipos, "equipos")

    # Sponsors de la temporada actual
    sponsors_actuales = schema.get("sponsors", 50)
    sponsors_a_borrar = random.randint(0,15)
    for _ in range(sponsors_a_borrar):
        random_sponsor = random.randint(0,len(sponsors_actuales))
        del sponsors_actuales(random_sponsor)
    # Creo nuevos sponsors
    nuevos_sponsors = generator.generate_sponsors(sponsors_a_borrar)
    
    # Esto es que algun nombre de sponsor no sea un nombre de equipo, 
    # Si lo es, lo borro de la lista_nueovs_sponsors, genero uno nuevo y lo apendeo
    for equipo in nuevos_equipos:
        for sponsor in nuevos_sponsors:
            if sponsor == equipo["Nombre"]:
                del nuevos_sponsors(sponsor)
                nuevo_sponsor_no_errado = generator.generate_sponsors(1)
                nuevos_sponsors.append(nuevo_sponsor_no_errado)

    schema.insert(sponsors_actuales + nuevos_sponsors, "sponsors")

    # Sponsorea en la temporada actual
    sponsorea_viejo = schema.get("sponsorea", 50)
    for sponsor in nuevos_sponsors:
        if sponsor["nombre"] in sponsorea_viejo:
            if sponsorea_viejo["Nombre_equipo"] in nuevos_equipos:
                # Copio sponsorea y le sumo un año al contrato. 
            else:
                # Se quedo el sponsor sin equipo.
                # Le asigno al azar un equipo
        else : 
            # Le asigno al azar un equipo (con más peso a los nuevos)


    # Maneja_para en la temporada actual
    maneja_para_viejo = schema.get("maneja_para", 20)
    for conductor in nuevos_conductores:
        if conductor["nombre"] and conductor["apellido"] in maneja_para_viejo:
            if maneja_para_viejo["Nombre_equipo"] in nuevos_equipos:
                # Copio maneja_para_viejo y le sumo un año al contrato. 
            else:
                # Se quedo el conductor sin equipo.
                # Le asigno un equipo libre
        else : 
            # Le asigno a un equipo libre


    
def generate_data_race(base_time: str):
    # Genera data en Mediciones, maneja_en y Ediciones
    # Tiene que importar el clima para modificar los resultados de la carrera. 
    # Generando distintas ramas que pueden pasar.   
    generator = DataGenerator()
    schema = Schema()

    climate = random.choice(["sunny", "rainy", "stormy"])
    




        
            





def generate_data(base_time: str, n: int):
    """Generates synth data and saves to DB.

    Parameters
    ----------
    base_time: strpoetry export --without-hashes --format=requirements.txt > requirements.txt

        Base datetime to start events from.
    n : int
        Number of events to generate.
    """
    generator = DataGenerator()
    schema = Schema()
    """ Mi idea de como hacerlo, en vez de tener muchas lineas de get_tabla, ponemos get(tabla) y así le pasamos por parametro.
    Esto es para escribir menos código en el schema.py
    """
    people = generator.generate_people(100)
    schema.insert(people, "people")

    people_sample = schema.get_people(100)
    sessions = generator.generate_sessions(
        people_sample,
        datetime.datetime.fromisoformat(base_time),
        datetime.timedelta(days=1),
        n,
    )
    schema.insert(sessions, "sessions")


with DAG(
    "fill_data",
    start_date=pendulum.datetime(2024, 6, 1, tz="UTC"),
    schedule_interval="@daily",
    catchup=True,
) as dag:
    op = PythonOperator(
        task_id="task",
        python_callable=generate_data,
        op_kwargs=dict(n=EVENTS_PER_DAY, base_time="{{ ds }}"),
    )




