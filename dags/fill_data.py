from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
import pendulum
import datetime
import random
from td7.data_generator import DataGenerator
from td7.schema import Schema

EVENTS_PER_DAY = 10_000

def generate_data_annually(base_time: str):
    # Inicializo al generador y a mi schema
    generator = DataGenerator()
    schema = Schema()

    # Agarro el año:
    year = int(schema.get_ano()) + 1

    # Conductores de la temporada actual
    # Elimino a (1-3) conductores actuales y creo 3 nuevos
    conductores_actuales = schema.get_conductores(20)
    conductores_a_borrar = random.randint(1,3)
    for i in range(conductores_a_borrar):
        random_conductor = random.randint(0,len(conductores_actuales)-1)
        conductores_actuales.pop(random_conductor)
        # Lo desactivo como activo en la base de datos
        schema.deactivate_conductor(conductores_actuales[random_conductor]["Id_conductor"])

    # Reemplazo a esos conductores
    nuevos_conductores = generator.generate_conductores(conductores_a_borrar)
    schema.insert(nuevos_conductores, "conductores")
    # Entonces tenemos 20 conductores con activo
    # Esto me sirve para después porque se relaciona con otros equipos.
    conductores_actuales = conductores_actuales + nuevos_conductores


    # Equipos de la temporada actual
    equipos_actuales = schema.get_equipos()
    equipos_a_borrar = random.randint(0,2)
    # tengo que borrar a sus sponsors primero
    for i in range(equipos_a_borrar):
        random_equipo = random.randint(0,len(equipos_actuales)-1)
        equipos_actuales.pop(random_equipo)
    # Creo nuevos equipos
    nuevos_equipos = generator.generate_equipos(equipos_a_borrar)
    # Lo añado a los equipos a devolver.
    schema.insert(nuevos_equipos, "equipos")
    equipos_actuales = equipos_actuales + nuevos_equipos


    # Sponsors de la temporada actual
    sponsors_actuales = schema.get("sponsors", 50)
    sponsors_a_borrar = random.randint(0,15)
    for _ in range(sponsors_a_borrar):
        random_sponsor = random.randint(0,len(sponsors_actuales)-1)
        sponsors_actuales.pop(random_sponsor)
    # Creo nuevos sponsors
    nuevos_sponsors = generator.generate_sponsors(sponsors_a_borrar)
    
    # Esto es un checkeo para que algun nombre de sponsor no sea un nombre de equipo rival, 
    # Si lo es, lo borro de la lista_nuevos_sponsors, genero uno nuevo y lo appendeo
    for equipo in equipos_actuales:
        for sponsor in nuevos_sponsors:
            if sponsor == equipo["Nombre"]:
                nuevos_sponsors.pop(sponsor)
                nuevo_sponsor_no_errado = generator.generate_sponsors(1)
                nuevos_sponsors.append(nuevo_sponsor_no_errado)

    schema.insert(nuevos_sponsors, "sponsors")

     # Sponsorea en la temporada actual
    sponsorea_viejo = schema.get_sponsorea_last_year("sponsorea", year) # sponsors de la anterior
    # la funcion toma el año actual y devuelve info del anterior
    sponsorea_nuevo = []
    for sponsor in nuevos_sponsors:
        if sponsor in [s["Nombre_sponsor"] for s in sponsorea_viejo]:
            sponsorea_viejo_sponsor = next(s for s in sponsorea_viejo if s["Nombre_sponsor"] == sponsor["Nombre"])
            if sponsorea_viejo_sponsor["Nombre_equipo"] in [e["Nombre"] for e in nuevos_equipos]:
                sponsorea_viejo_sponsor["Ano_contrato"] += 1
                sponsorea_nuevo.append(sponsorea_viejo_sponsor)
            else:
                nuevo_equipo = random.choice(equipos_actuales)
                sponsorea_nuevo.append({
                    "Nombre_sponsor": sponsor["Nombre"],
                    "Nombre_equipo": nuevo_equipo["Nombre"],
                    "Ano_contrato": year
                })
        else:
            nuevo_equipo = random.choice(equipos_actuales)
            sponsorea_nuevo.append({
                "Nombre_sponsor": sponsor["Nombre"],
                "Nombre_equipo": nuevo_equipo["Nombre"],
                "Ano_contrato": year
            })
    schema.insert(sponsorea_nuevo, "sponsorea")



    # Maneja_para en la temporada actual
    maneja_para_viejo = schema.get_maneja_para_last_year("maneja_para", 20, year)
    maneja_para_nuevo = []
    for conductor in nuevos_conductores:
        if conductor in [m["Id_conductor"] for m in maneja_para_viejo]:
            maneja_para_viejo_conductor = next(m for m in maneja_para_viejo if m["Id_conductor"] == conductor["id"])
            if maneja_para_viejo_conductor["Nombre_equipo"] in [e["Nombre"] for e in nuevos_equipos]:
                maneja_para_viejo_conductor["Ano_contrato"] += 1
                maneja_para_nuevo.append(maneja_para_viejo_conductor)
            else:
                nuevo_equipo = random.choice(equipos_actuales)
                maneja_para_nuevo.append({
                    "Id_conductor": conductor["id"],
                    "Nombre_equipo": nuevo_equipo["Nombre"],
                    "Ano_contrato": year
                })
        else:
            nuevo_equipo = random.choice(equipos_actuales)
            maneja_para_nuevo.append({
                "Id_conductor": conductor["id"],
                "Nombre_equipo": nuevo_equipo["Nombre"],
                "Ano_contrato": year
            })
    schema.insert(maneja_para_nuevo, "maneja_para")


    # Genero Vehículos
    nuevos_vehiculos = generator.generate_vehiculos(20, equipos_actuales)
    schema.insert(nuevos_vehiculos)


    

def generate_data_race(base_time: str):
    # Genera data en Mediciones, maneja_en, Ediciones y participan_en
    # Tiene que importar el clima para modificar los resultados de la carrera. 
    # Generando distintas ramas.  
    generator = DataGenerator()
    schema = Schema()

    # Agarro de mi DB los datos de la season actual que necesito para crear. 
    conductores_actuales = schema.get_conductores(20)
    vehiculos_actuales = schema.get_vehiculos_last_year(20) # ultimos 20 vehiculos son los que queremos

    year = schema.get_ano() # Este es el año actual que sacamos de Sponsorea que se genera a principio de cada año.

    # Hay que checkear q la carrera no esta en las corridas hasta ahora
    carreras_de_la_season = schema.get_carreras_actuales("Ediciones", year)
    gps = schema.get("GP") 
    for carrera in gps:
        if carrera not in carreras_de_la_season:
            # Encontré un GP a generar
            carrera_actual = generator.generate_una_edicion(carrera, year, len(carreras_de_la_season)-1)
            break
    schema.insert(carrera_actual, "Ediciones")
    
    
    # Factor externo que afecta la generación de las mediciones (pq afecta la carrera)
    climate = random.choice(["sunny", "rainy"]) 
    # Tengo que hacer que busque el clima de buenos aires cada vez, 
    # solo con tener sunny y rainy es suficiente pero hay q usar una api para sacar el clima actual de bsas


    mediciones_carrera = generator.generate_mediciones(vehiculos_actuales, carrera_actual, climate)
    schema.insert(mediciones_carrera, "mediciones")

    maneja_en = generator.generate_maneja_en(conductores_actuales, carrera_actual, vehiculos_actuales)
    schema.insert(maneja_en)

    equipos_actuales = schema.get_equipos() 
    participan_en = generator.generate_participan_en(equipos_actuales, carrera_actual)
    schema.insert(participan_en)



with DAG(
    "fill_data",
    start_date=pendulum.datetime(2024, 6, 1, tz="UTC"),
    schedule_interval=None,
    catchup=False,
) as dag:

    generate_annual_data = PythonOperator(
        task_id="generate_data_annually",
        python_callable=generate_data_annually,
        op_kwargs=dict(base_time="{{ ds }}"),
    )

    generate_race_data = PythonOperator(
        task_id="generate_data_race",
        python_callable=generate_data_race,
        op_kwargs=dict(base_time="{{ ds }}"),
    )

    # Chain the tasks so generate_race_data runs 22 times between each generate_annual_data
    with TaskGroup("race_data_group") as race_data_group:
        race_data_tasks = [
            PythonOperator(
                task_id=f"generate_data_race_{i+1}",
                python_callable=generate_data_race,
                op_kwargs=dict(base_time="{{ ds }}"),
            ) for i in range(22)
        ]

    generate_annual_data >> race_data_group
    for task in race_data_tasks:
        generate_annual_data >> task

    # Ensure the schedule interval is correct by scheduling manually
    dag_schedule = DAG(
        "fill_data_scheduler",
        start_date=days_ago(1),
        schedule_interval="0 0 */23 * *",
        catchup=False,
    )

    with dag_schedule:
        schedule_generate_annual_data = PythonOperator(
            task_id="schedule_generate_data_annually",
            python_callable=generate_data_annually,
            op_kwargs=dict(base_time="{{ ds }}"),
        )

        schedule_generate_race_data = PythonOperator(
            task_id="schedule_generate_data_race",
            python_callable=generate_data_race,
            op_kwargs=dict(base_time="{{ ds }}"),
        )

        schedule_generate_annual_data >> schedule_generate_race_data




