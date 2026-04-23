## Coordinacion de instancias SUM

### El Problema de Sincronización
Las N instancias de Sum comparten una única cola de entrada. 
Cada mensaje de datos puede ser consumido por cualquier instancia, de modo que el trabajo se distribuye naturalmente. 
Sin embargo, el mensaje de fin de ingesta (EOF) llega a una sola instancia 
—la que lo consuma primero— y contiene el total de registros enviados por el cliente (total_chunks).
El desafío es que cada instancia de Sum solo conoce cuántos chunks procesó ella misma, pero no el estado de las demás.
Se debe coordinar que cada instancia de sum envie su resultado parcial solo cuando haya terminado de procesar todos los
datos del cliente que se le fueron asignados incluyendo los ya procesados, los que se estan procesando en el instante
de llegar el EOF y los almacenados en el prefetch.
Para que todas las instancias puedan enviar su resultado parcial al Aggregator de forma coordinada se implementa
un algoritmo distribuido token rign.

### Funcionamiento del Anillo
Las N instancias de Sum se organizan lógicamente en un anillo, donde la instancia i envía su token a la instancia
(i+1) % N. 
El flujo es el siguiente:
- Cuando una instancia de Sum recibe el mensaje EOF de un cliente extrae el id y la cantidad total de chunks producidos.
- Construye un token con [client_id, accumulated_count, total_chunks], donde "accumulated_count" es la cantidad de 
chunks que la instancia ha procesado hasta el momento, y "total_chunks" es el total de registros del cliente y lo envía
a la siguiente instancia del anillo.
- Cada instancia que recibe el token agrega a "accumulated_count" la cantidad de chunks que ha procesado y lo reenvía
al siguiente nodo del anillo. Para evitar sumar chunks que ya estaban en el acumulado, cada instancia mantiene un
contador de chunks "contributed". La logica es restar el valor de "contributed" al contador de chunks procesados 
y luego actualizar "contributed" con el valor actual del contador de chunks procesados y sumarlo al acumulado del token.
- Cuando una instancia recibe el token y el valor de "accumulated_count" es igual a "total_chunks", significa que todas
las instancias en su conjunto han procesado todos los datos del cliente. En ese momento, la instancia hace un flush de
su resultado parcial al Aggregator y marca un flag interno indicando que ya envió su resultado parcial.
- Si el token llega a una instancia que ya envió su resultado parcial, esa instancia simplemente lo descarta. Al estar
en un anillo, el nodo siguiente el unico estado posible que tiene es haber hecho flush y como llego desde otra instancia,
esta ultima tambien hizo el chequeo del acumulado y el total, por lo que tambien hizo flush, por lo que el token se 
puede descartar sin riesgo de perder información relevante.

## Coordinación de Instancias Aggregation

### Distribución por Hash de Fruta
El problema de coordinación entre las instancias de Aggregator es que cada una de ellas recibe datos de todas las 
instancias de Sum y debemos asegurar que los datos de una misma fruta para un mismo cliente se haga en el mismo 
Aggregator.
Para asegurar esto, cada instancia de Sum envía los pares (fruta, cantidad) a un Aggregator específico según el hash
MD5 del nombre de la fruta:
index = int(hashlib.md5(fruit.encode()).hexdigest(), 16) % AGGREGATION_AMOUNT
Como el envio de datos para un cliente se hace en un unico momento esto garantiza que todos los datos de una misma fruta
para un mismo cliente siempre llegan al mismo Aggregator.

### Conteo de EOFs
Para coordinar la recepcion de la totalidad de datos de un cliente a los distintos Aggregators, se implementa un conteo
de EOFs.
Cada instancia de Sum envía un EOF a cada uno de los M Aggregators al finalizar el flush de un cliente sin importar si
envian datos de frutas o no. Por lo tanto, cada Aggregator espera recibir exactamente N EOFs (uno por cada instancia 
de Sum) antes de calcular su top parcial y enviarlo al Joiner. 
El conteo se lleva por client_id en el diccionario eof_count_by_client.

## Coordinación del Joiner

### Conteo de EOFs
El Joiner recibe tops parciales de los M Aggregators. Para coordinar la recepción de la totalidad de los tops parciales
de un cliente, se implementa un conteo de EOFs como en el caso de los aggregators.

## Escalabilidad del Sistema

### Escalabilidad respecto a Clientes
Cada cliente que se conecta al Gateway recibe un client_id único (UUID). Este identificador acompaña a todos los 
mensajes a lo largo del pipeline, lo que permite que múltiples clientes sean procesados de forma concurrente sin 
interferencia. Los controles Sum, Aggregation y Joiner mantienen estado separado por client_id.

### Escalabilidad respecto a Controles Sum
Agregar instancias de Sum (aumentar SUM_AMOUNT) distribuye la carga de procesamiento de datos entre más procesos. 
El algoritmo de token ring escala linealmente: a más instancias, el token da más vueltas, pero el trabajo por vuelta 
es constante.

### Escalabilidad respecto a Controles Aggregation
Agregar instancias de Aggregation (aumentar AGGREGATION_AMOUNT) distribuye la suma de cantidad por fruta entre más 
procesos. Dado que la asignación se realiza por hash, la distribución es aproximadamente uniforme. 

### Escalabilidad respecto a Controles Joiner
El Joiner puede ser el cuello de botella, ya que recibe los tops parciales de todos los Aggregators. 
Sin embargo, dado que el top parcial es de tamaño fijo (pj, top 3), el volumen de datos que maneja el Joiner no crece
con la cantidad de clientes o los datos de este sino que lo hace linealmente por cantidad de agregators N * k-top, 
lo que permite cierta escalabilidad.
