
# PySpark


Para esta práctica vamos a utilizar pySpark con el objetivo de leer un fichero .csv utilizando diferentes tipos de abstracciones de datos. Se ofrecen las siguientes:

- RDD
- DataFrame
- DataSet

Pero nos vamos a centrar exclusivamente en dos de ellas, RDD y DataFrame.


***Spark RDD APIs***: Viene del acrónimo RDD(Resilient Distributed Datasets). RDD es fundamental en la estructura de datos de Spark. Permite al programador realizar en memoria de grandes cluster con tolerancia a fallos.


***Spark Dataframe APIs***: A diferencia de RDD, los datos se organizan dentro de columnas con sus respectivos nombres. Como ocurre para una tabla en una base de datos relacional. DataFrame permite a los desarrolladores imponer una estructura de datos que es inmutable, permitiend una mayor abstracción.


## Spark RDD

En primer lugar vamos a establecer nuestro *SparkConf*, que se lo pasaremos al *SparkContext*. Esto configurará algunas propiedades estandar para poder usar Spark.

### create_pair:

Esta función devolvera una lista cuya primera posición tendrá una lista con todos los topics diferentes, y en la segunda los valores acumulados correspondientes a cada uno de ellos. Esta lista se usará para representar los datos graficamente en la siguiente función.


```python
import plotly
from pyspark import SparkConf, SparkContext
spark_conf = SparkConf()
spark_context = SparkContext(conf=spark_conf)
```

Then we have to log into spark. 


```python
logger = spark_context._jvm.org.apache.log4j
logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)
```

Then we get the values that we are interested in, by this lambda expresion.


```python
diseases = spark_context \
        .textFile('U.S._Chronic_Disease_Indicators__CDI_.csv')\
        .map(lambda line: line.split(","))\
        .map(lambda list: (list[5], 1))\
        .reduceByKey(lambda x, y: x + y)\
        .sortBy(lambda pair: pair[1])
```

*lambda* crea funciones anónimas con la función que se le define después de los dos puntos.

- Se lee el archivo con el *.csv* de enfermedades que vamos a analizar.
- Se separan como lineas lo que haya entre el delimitador indicado *,*, la función *map* lo devuelve en un *dataset*
- Se elige la posición 5 de la lista separada por comas (*topic*), y se coge un elemto de cada una de ellas.
- en *reduceByKey* se crea una función de dos variables donde una de ellas actua como la clave y su valor acumulado actual, y la otra como el siguiente valor que se acumulará a esa clave. Así, se va acumulando para cada clave (*topic*) la cantidad acumulada total.
- *pair[1]* contiene la cantidad acumulada de cada clave que actua como valor de clave por los cuales se ordenará el *RDD*




```python
result = diseases \
        .collect()
```

*collect()* devuelve una lista que contiene todos los elementos del *RDD* creado, en este caso se trata de una lista que en cada posición contiene un *pair*, es decir, topic con su cantidad acumulada correspondiente.


Queremos representar los datos en un diagrama de quesitos, para lo que usaremos *plotly*.
Para ello, necesitaremos los datos del *RDD* distribuidos en dos listas, una de ellas con la columna de topics, y otra con la de resultados.


```python
topic = list()
count =list()
for pair in result:
    print(pair)
    topic.append(pair[0])
    count.append(pair[1])
```

    ('Topic', 1)
    (' NVSS"', 52)
    (' Mortality"', 104)
    (' UDS"', 220)
    ('Reproductive Health', 2060)
    ('Disability', 2968)
    ('Immunization', 5220)
    ('Mental Health', 7225)
    ('Oral Health', 10917)
    ('Chronic Kidney Disease', 12395)
    ('Cancer', 14101)
    ('Older Adults', 15300)
    ('Tobacco', 29306)
    ('Alcohol', 31850)
    ('"Nutrition', 33621)
    ('Overarching Conditions', 39258)
    ('Asthma', 39261)
    ('Arthritis', 41765)
    ('Cardiovascular Disease', 75787)
    ('Chronic Obstructive Pulmonary Disease', 78729)
    ('Diabetes', 79579)


Para conseguir estas dos listas con bastará con recorrer todos los pares guardados en el resultado y coger la primera posición para la clave y la segunda para su valor acumulado correspondiente, almacenándolos en listas.


```python
spark_context.stop()
```

Para terminar con esta función, paramos *spark* y devolvemos una lista con los valores *[topic, count]*

### make_plot

Esta función nos dará un análisis visual de los datos mediante un diagrama de quesitos. Recibirá como argumentos la salida de la función anterior y el nombre del archivo de salida que creará en formato *html*. Con ello llamará a la función *plot* de *plotly* que creará una imagen en formato *.png*


```python
trace = plotly.graph_objs.Pie(labels=topic, values=count)
plotly.offline.plot([trace], filename="outputfile.html", image='png')
```




    'file:///Users/nairachiclana/Documents/bitBucket/enfermedades-cronicas-spark/outputfile.html'




```python
class Diseases:
    def __init__(self, file_name):
        make_plot(create_pair(file_name),file_name)

    def create_pair(self,file_name):
        return create_pair(file_name)

    def make_plot(self, pair, salida):
        return make_plot(pair, salida)

```

![primera.png](primera.png)

Finalmente, crearemos una clase que invocará la ejecución de estas dos funciones.

## spark Dataframe

Para el caso de dataFrame, al igual que antes, empezamos importando las librerías correspondientes. 

El codigo está estructurado en diferentes funciones, en primer lugar encontramos el main.


```python
import sys
from collections import OrderedDict
from pyspark.sql import SparkSession
import plotly

def main(file_path):
    # Execute main function
    process_disease_indicators(file_path)
```

La segunda función se llama spark_dataframe y lo que hace es abrir una conexión de Spark, y devolver un dataFrame con los datos del archivo que se le halla pasado.


```python
def spark_dataframe(csv_file_path):
    # Initialize spark session
    spark_session = SparkSession \
        .builder \
        .getOrCreate()

    logger = spark_session._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    # Create Spark Data frame
    data_frame = spark_session \
        .read \
        .format("csv") \
        .options(header='true', inferschema='true') \
        .load(csv_file_path)
    return data_frame
```

También hemos diseñado otra función que la vamos a utilizar para plotear los resultados obtenidos. Esta función genera una imagen HTML utilizando la librería Plotly para un valor dado de diccionario.


```python
def plot_pie(data_dict, filepath_out):
    # Add labels
    labels = list(data_dict.keys())
    # Add values
    values = list(data_dict.values())
    trace = plotly.graph_objs.Pie(labels=labels, values=values)
    # Plot graph
    plotly.offline.plot([trace], filename=filepath_out, image='png')
```

Pasamos a ver la función principal que es la que llamamos en el main. En ella, en primer lugar llamamos a la función necesaria para crear el dataFrame donde contamos los diferentes pares diferentes.


```python
def process_disease_indicators(file_path):
    # Create data frame
    data_frame = spark_dataframe(file_path)
    result_set = OrderedDict()
    # Collect results and add them to an OrderedDict
    for pair in list(data_frame.groupBy("Topic").count().collect()):
        result_set[pair[0]] = pair[1]
    plot_pie(result_set, 'data_frame_results.html')
```


```python
process_disease_indicators('U.S._Chronic_Disease_Indicators__CDI_.csv')
```

![segunda.png](segunda.png)
