-- Guardar el valor actual y habilitar temporalmente
SET @old_local_infile := @@GLOBAL.local_infile;
SET GLOBAL local_infile = 1;

-- ================================================================
-- SCRIPT DE CREACIÓN Y CARGA DEL DATA WAREHOUSE NETFLIX
-- Basado en Metodología Hefesto - Esquema Estrella
-- ================================================================

USE netflix;

-- ================================================================
-- PASO 1: ELIMINAR TABLAS SI EXISTEN (para recargas)
-- ================================================================

DROP TABLE IF EXISTS fact_rating;
DROP TABLE IF EXISTS dim_fecha;
DROP TABLE IF EXISTS dim_usuario;
DROP TABLE IF EXISTS dim_pelicula;

-- ================================================================
-- PASO 2: CREAR DIMENSIONES
-- ================================================================

-- Dimensión PELÍCULA
CREATE TABLE dim_pelicula (
    movie_id INT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    year_of_release SMALLINT NULL,
    INDEX idx_year (year_of_release)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Dimensión de películas del catálogo Netflix';

-- Dimensión USUARIO
CREATE TABLE dim_usuario (
    customer_id INT PRIMARY KEY
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Dimensión de usuarios (anonimizados)';

-- Dimensión FECHA
CREATE TABLE dim_fecha (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    year SMALLINT NOT NULL,
    quarter TINYINT NOT NULL,
    month TINYINT NOT NULL,
    day TINYINT NOT NULL,
    week_of_year TINYINT NOT NULL,
    INDEX idx_year (year),
    INDEX idx_month (year, month),
    INDEX idx_quarter (year, quarter),
    INDEX idx_full_date (full_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Dimensión de tiempo con jerarquías: Año→Trimestre→Mes→Día';

-- ================================================================
-- PASO 3: CREAR TABLA DE HECHOS
-- ================================================================

CREATE TABLE fact_rating (
    customer_id INT NOT NULL,
    movie_id INT NOT NULL,
    date_key INT NOT NULL,
    rating_value TINYINT NOT NULL CHECK (rating_value BETWEEN 1 AND 5),
    PRIMARY KEY (customer_id, movie_id, date_key),
    FOREIGN KEY (customer_id) REFERENCES dim_usuario(customer_id),
    FOREIGN KEY (movie_id) REFERENCES dim_pelicula(movie_id),
    FOREIGN KEY (date_key) REFERENCES dim_fecha(date_key),
    INDEX idx_movie (movie_id),
    INDEX idx_date (date_key),
    INDEX idx_rating (rating_value)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Tabla de hechos: cada registro representa una calificación';

-- ================================================================
-- PASO 4: CARGAR DATOS DESDE CSV
-- ================================================================

-- IMPORTANTE: Asegúrate de que MySQL tenga permisos para leer archivos
-- y que la variable secure_file_priv lo permita.
-- Verifica con: SHOW VARIABLES LIKE 'secure_file_priv';
-- 
-- Si secure_file_priv está configurado, copia los archivos CSV a esa carpeta
-- o ajusta las rutas según tu configuración.

-- Cargar dimensión PELÍCULA
LOAD DATA LOCAL INFILE 'C:/Users/user/Desktop/Base de Datos 2/ProyectoBD2/output_dw/dim_pelicula.csv'
INTO TABLE dim_pelicula
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
ESCAPED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(movie_id, year_of_release, title);

-- Cargar dimensión USUARIO
LOAD DATA LOCAL INFILE 'C:/Users/user/Desktop/Base de Datos 2/ProyectoBD2/output_dw/dim_usuario.csv'
INTO TABLE dim_usuario
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(customer_id);

-- Cargar dimensión FECHA
LOAD DATA LOCAL INFILE 'C:/Users/user/Desktop/Base de Datos 2/ProyectoBD2/output_dw/dim_fecha.csv'
INTO TABLE dim_fecha
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(date_key, full_date, year, quarter, month, day, week_of_year);

-- Cargar tabla de HECHOS
LOAD DATA LOCAL INFILE 'C:/Users/user/Desktop/Base de Datos 2/ProyectoBD2/output_dw/fact_rating.csv'
INTO TABLE fact_rating
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(customer_id, movie_id, date_key, rating_value);

-- ================================================================
-- PASO 5: VERIFICAR CARGA
-- ================================================================

SELECT 'Dimensión Película' AS tabla, COUNT(*) AS registros FROM dim_pelicula
UNION ALL
SELECT 'Dimensión Usuario', COUNT(*) FROM dim_usuario
UNION ALL
SELECT 'Dimensión Fecha', COUNT(*) FROM dim_fecha
UNION ALL
SELECT 'Tabla Hechos Rating', COUNT(*) FROM fact_rating;

-- ================================================================
-- PASO 6: CONSULTAS DE VALIDACIÓN
-- ================================================================

-- Top 10 películas con mejor rating promedio (mínimo 100 calificaciones)
SELECT 
    p.title,
    p.year_of_release,
    COUNT(*) AS total_ratings,
    AVG(f.rating_value) AS avg_rating,
    MIN(f.rating_value) AS min_rating,
    MAX(f.rating_value) AS max_rating
FROM fact_rating f
JOIN dim_pelicula p ON f.movie_id = p.movie_id
GROUP BY p.movie_id, p.title, p.year_of_release
HAVING total_ratings >= 100
ORDER BY avg_rating DESC, total_ratings DESC
LIMIT 10;

-- Distribución de ratings
SELECT 
    rating_value,
    COUNT(*) AS cantidad,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fact_rating), 2) AS porcentaje
FROM fact_rating
GROUP BY rating_value
ORDER BY rating_value;

-- Evolución anual de ratings
SELECT 
    d.year,
    COUNT(*) AS total_ratings,
    AVG(f.rating_value) AS avg_rating,
    COUNT(DISTINCT f.customer_id) AS usuarios_activos,
    COUNT(DISTINCT f.movie_id) AS peliculas_calificadas
FROM fact_rating f
JOIN dim_fecha d ON f.date_key = d.date_key
GROUP BY d.year
ORDER BY d.year;

-- Top 10 usuarios más activos
SELECT 
    customer_id,
    COUNT(*) AS total_calificaciones,
    AVG(rating_value) AS rating_promedio
FROM fact_rating
GROUP BY customer_id
ORDER BY total_calificaciones DESC
LIMIT 10;

-- ================================================================
-- FIN DEL SCRIPT
-- ================================================================

SELECT '✅ Data Warehouse cargado exitosamente' AS status;

-- Restaurar el valor anterior
SET GLOBAL local_infile = @old_local_infile;
