-- Crear tabla de Conductores
CREATE TABLE Conductores (
    Id_conductor SERIAL PRIMARY KEY,
    apellido VARCHAR(255) NOT NULL,
    nombre VARCHAR(255) NOT NULL,
    nacionalidad VARCHAR(100) NOT NULL,
    fecha_nacimiento DATE NOT NULL,
    activo BOOLEAN
); 
-- creo una flag activo para ver si el conductor esta activo en la temporada o no

-- Crear tabla de Equipos
CREATE TABLE Equipos (
    Nombre VARCHAR(255) PRIMARY KEY,
    Pais VARCHAR(100) NOT NULL,
    Ano_creacion INT CHECK (Ano_creacion BETWEEN 1800 AND EXTRACT(YEAR FROM CURRENT_DATE))
);

-- Crear tabla de Sponsors
CREATE TABLE Sponsors (
	Nombre VARCHAR(255) PRIMARY KEY
);

-- crear tabla de interrelacion entre Equipos y Sponsors: Sponsporea
CREATE TABLE Sponsorea (
    Nombre_sponsor VARCHAR(255),
    Nombre_equipo VARCHAR(255),
    Ano_contrato INT,
    PRIMARY KEY (Nombre_sponsor, Nombre_equipo),
    FOREIGN KEY (Nombre_equipo) REFERENCES Equipos(Nombre),
    FOREIGN KEY (Nombre_sponsor) REFERENCES Sponsors(Nombre)
);

-- Crear tabla de GP
CREATE TABLE GP (
	Nombre VARCHAR(255) UNIQUE,
    longitud INT CHECK (longitud BETWEEN 3500 AND 7000),
    pais VARCHAR(100),
	PRIMARY KEY (Nombre)
);

-- Crear tabla de la edicion de un GP definida por el ano
CREATE TABLE Ediciones (
    Ano_carrera INT CHECK (Ano_carrera BETWEEN 1800 AND EXTRACT(YEAR FROM CURRENT_DATE)),
    Nombre_carrera VARCHAR(255),
    nro_carrera INT CHECK (nro_carrera BETWEEN 1 AND 50),
    vuelta_record INT CHECK (vuelta_record BETWEEN 0 AND 300),
    PRIMARY KEY (Ano_carrera, Nombre_carrera), 
    FOREIGN KEY (Nombre_carrera) REFERENCES GP(Nombre)
);


-- Crear tabla de Participan_en
CREATE TABLE Participan_en (
    Nombre_equipo VARCHAR(255),
    Ano_carrera INT,
    Nombre_carrera VARCHAR(255),
    PRIMARY KEY (Nombre_equipo, Ano_carrera, Nombre_carrera),
    FOREIGN KEY (Nombre_equipo) REFERENCES Equipos(Nombre),
    FOREIGN KEY (Ano_carrera, Nombre_carrera) REFERENCES Ediciones(Ano_carrera, Nombre_carrera)
);


-- Crear tabla de Maneja_para
CREATE TABLE Maneja_para (
    Id_conductor INT,
    Nombre_equipo VARCHAR(255),
    ano INT CHECK (ano BETWEEN 1800 AND EXTRACT(YEAR FROM CURRENT_DATE)),
    PRIMARY KEY (Id_conductor, Nombre_equipo, ano),
    FOREIGN KEY (Id_conductor) REFERENCES Conductores(Id_conductor),
    FOREIGN KEY (Nombre_equipo) REFERENCES Equipos(Nombre)
);

-- Crear tabla de Maneja_en
CREATE TABLE Maneja_en (
    ID_conductor INT,
    Ano_carrera INT,
    Nombre_carrera VARCHAR(255),
    ID_Vehiculo INT,
    posicion_inicial INT,
    posicion INT,
    PRIMARY KEY (ID_conductor, Ano_carrera, Nombre_carrera),
    FOREIGN KEY (ID_conductor) REFERENCES Conductores(Id_conductor),
    FOREIGN KEY (Ano_carrera, Nombre_carrera) REFERENCES Ediciones(Ano_carrera, Nombre_carrera)
);

-- Crear tabla de Vehiculos
CREATE TABLE Vehiculos (
    Id_Vehiculo SERIAL PRIMARY KEY,
    Nombre_Equipo VARCHAR(255),
    Motor VARCHAR(255),
    Modelo VARCHAR(255),
    FOREIGN KEY (Nombre_Equipo) REFERENCES Equipos(Nombre)
);

-- Crear tabla de Puntajes
CREATE TABLE Puntajes (
    posicion INT PRIMARY KEY,
    puntos INT NOT NULL
);

-- Insertar valores de puntos según posición
INSERT INTO Puntajes (posicion, puntos) VALUES
(1, 25), (2, 18), (3, 15), (4, 12), (5, 10),
(6, 8), (7, 6), (8, 4), (9, 2), (10, 1);

-- Crear tabla de Mediciones
CREATE TABLE Mediciones (
    Id_Vehiculo INT,
    Ano_carrera INT,
    Nombre_carrera VARCHAR(255),
    tiempo_carrera TIMESTAMP, -- tiempo
    tipo VARCHAR(30), -- hay que anadir una buena CONSTRAINT para que solo sean los strings: temp_motor, presion_ruedas, temp_cabina
    medicion FLOAT CHECK (medicion BETWEEN -100 AND 4000),
    PRIMARY KEY (Id_vehiculo, Ano_carrera, Nombre_carrera, tiempo_carrera),
    FOREIGN KEY (Id_Vehiculo) REFERENCES Vehiculos(Id_Vehiculo),
    FOREIGN KEY (Ano_carrera, Nombre_carrera) REFERENCES Ediciones(Ano_carrera, Nombre_carrera)
);
-- en Mediciones, en vez de medicion si pongo ts, carrera la dejamos siendo debil y el ts reemplaza a la clave surrogada.
-- Mide es ahora innecesario y lo saque

-- elimine la condicion redundante del WHERE
CREATE OR REPLACE FUNCTION check_driver_team_consistency()
RETURNS TRIGGER AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM Maneja_para mp
        JOIN Vehiculos v ON mp.Nombre_equipo = v.Nombre_Equipo
        WHERE mp.Id_conductor = NEW.ID_conductor AND mp.ano = NEW.Ano_carrera
              AND v.Id_Vehiculo = NEW.ID_Vehiculo
    ) THEN
        RAISE EXCEPTION 'El conductor no está autorizado a manejar este vehículo este año.';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_driver_team_before_insert
BEFORE INSERT OR UPDATE ON Maneja_en
FOR EACH ROW EXECUTE FUNCTION check_driver_team_consistency();

