# Arquitectura general

Se basa en un sistema de n clientes Go que, en la lógica de negocio, simulan ser agencias de lotería entregando sus apuestas a un servidor Python. Los clientes leen una serie de archivos csv, lugar donde están las apuestas realizadas en cada una de las agencias. 

El servidor recibe estas apuestas y, en base al número ganador que tiene predefinido, envía un mensaje a la agencia donde haya jugado esta persona (mediante el mismo socket por el cual se enviaron las apuestas en primera instancia). 
El servidor expone un name del servicio + puerto (server:5678 en mi caso), que establecemos en su docker compose, al que se conectan los clientes. Esta comunicación se realiza mediante TCP.
El flujo en líneas generales es: 
1. Lectura del csv de apuestas
2. Envío de batches de apuestas (para poder aprovechar el envío y enviar más de una apuesta)
3. Arribo de los mensajes al servidor
4. Se genera un quorum mínimo para proseguir luego de recibir el mensaje de final de cierta agencia.
5. Se envian los ganadores a sus respectivas agencias

# Protocolo de comunicación

Se sigue un protocolo del tipo TLV.
En el caso de esta implementación se realiza con un type de 1 byte, el cual puede ser:
- END
Mensaje enviado al servidor una vez se terminan de enviar todas las apuestas de dicha agencia.
También se envía desde servidor a cliente al terminar de enviar ganadores.
- WINNER
Mensaje recibido por la agencia y enviado por el servidor, indicando un ganador. Lógicamente, al poder haber más de un ganador, se envía un mensaje de este tipo
por ganador.
- BET
Mensaje legacy que se decidió mantener a modo de mostrar los mensajes que se usaban previo a batching y como una manera de documentar mi progreso en los ejercicios del Trabajo práctico.
- BETS_BATCH
Batch de apuestas enviado por las agencias, su objetivo es optimizar la cantidad de mensajes enviados por el socket
- BATCH_ACK
Al ser un batch de apuestas, es fundamental sincronizar la recepción de la información. Por ese motivo al recibir el conjunto de apuestas, el server envia un mensaje del tipo BATCH_ACK a la agencia emisora. Si bien la conexión TCP ya garantiza entrega ordenada mientras la conexión sea válida, ir mandando ACK's por cada batch procesado, proporciona un comportamiento mas armónico en el procesamiento y el posterior envio de más información de parte del cliente.
- ERROR
Ocurrió un error en el envio de información

Luego existe el length, el cual es representado con 4 bytes. Y por último, el payload que es lógicamente variable.
A su vez, dentro del payload, los campos de texto utilizan un prefijo de longitud de 2 bytes, mientras que los valores numéricos tienen un tamaño fijo según su tipo. Aquí hay un esquema de una secuencia de ejemplo completa:

[Secuencia de ejemplo](./secuencia_ejemplo.txt)

Ahí se puede ver el espacio que ocupa cada tipo de dato. En el encabezado general, el tipo de mensaje ocupa 1 byte y la longitud total del payload ocupa 4 bytes. Dentro del payload de `BETS_BATCH`, la cantidad de apuestas y la longitud individual de cada apuesta ocupan 4 bytes cada una.

Cada apuesta se representa de la siguiente manera:

| Campo | Representación | Tamaño |
|---|---|---|
| `agency_id` | Entero sin signo (`uint32`) | 4 bytes |
| Longitud del nombre | Entero sin signo (`uint16`) | 2 bytes |
| Nombre | Texto codificado en UTF-8 | Cantidad de bytes indicada por su longitud |
| Longitud del apellido | Entero sin signo (`uint16`) | 2 bytes |
| Apellido | Texto codificado en UTF-8 | Cantidad de bytes indicada por su longitud |
| `document` | Entero sin signo (`uint64`) | 8 bytes |
| Longitud de la fecha de nacimiento | Entero sin signo (`uint16`) | 2 bytes |
| Fecha de nacimiento | Texto codificado en UTF-8 | Cantidad de bytes indicada por su longitud |
| `number` | Entero sin signo (`uint32`) | 4 bytes |

Todos los valores numéricos se codifican en orden de bytes big-endian. En el ejemplo, la apuesta ocupa 52 bytes, el payload completo del batch ocupa 60 bytes y el mensaje TLV, incluyendo su encabezado, ocupa 65 bytes. Esta información permite al servidor recorrer y decodificar el batch correctamente, independientemente de cómo TCP divida el flujo de bytes.

# Procesamiento por batches y control de flujo

Como ya se había mencionado anteriormente, el envío de batches de apuestas permite reducir el overhead en la comunicación. 
El tamaño de este batch se define con BATCH_SIZE, este es el tamaño máximo de apuestas que intentará agrupar el cliente (aunque el payload también está restringido por el máximo de 16 MiB). 
Para coordinación de mensajes, el cliente espera el mensaje BATCH_ACK antes de enviar el siguiente batch. Por su lado, el servidor valida el batch y lo almacena antes de confirmarlo con el ACK. En el caso de que alguna apuesta sea inválida, no se envía ACK, sino ERROR.

# Concurrencia y quorum

El servidor posee un thread principal, el cual funciona como receptor de conexiones entrantes. Acto seguido de aceptar una conexión --> Crea un worker no-daemon por cliente.

Se podría ver como: accept() --> crea worker --> vuelve a accept()

Luego c/worker procesa la respectiva conexión por la que fue creado. Esto permite procesar varias agencias en simultáneo.
El trabajo es aislado, ya que cada worker posee:
- Socket
- Su tempDir
- su instancia de Lottery
- Agency id propio
- Sus apuestas

El único estado compartido, que es relevante a la hora de hacer el quorum, es el set de agencias finalizadas.
Este set está bueno para asegurarnos de no poner la misma agencia 2 veces.
Por otro lado, el mínimo requerido para aceptar el quorum es el valor configurable de AGENCY_QUORUM_MIN.

Igualmente este set (_completed_agencies) se protege con threading.Condition, el cual lo posee internamente _quorum_condition. Entonces cuando se envía un mensaje END, lo que ocurre es lo siguiente:

1. Worker toma el lock de _quorum_condition
2. Agrega la agencia al set
3. Comprueba si se alcanzó quorum
4. Si falta, queda bloqueado con wait_for()
5. Si se alcanzó, ejecuta notify_all()
6. Todos los workers despiertan y envían los ganadores

Este wait_for() espera de manera bloqueante, evitando el busy waiting. 

# Terminación graceful ante SIGTERM

Docker envía una señal SIGTERM cuando ejecuta ``docker compose stop -t 5``.
En ese momento, el proces tiene hasta 5 segundos para cerrar archivos, threads y sockets. En caso de que no llegue, Docker envía SIGKILL, que no da lugar a ninguna limpieza.

El flujo para el servidor es el siguiente:

1. Handler solicita shutdown
2. Marca threading.Event
3. Cierra socket listener
4. Ya no se bloquea accept()
5. Se cierran sockets activos
6. Recv/send se desbloquean
7. Notify_all() despierta workers que pudieran quedar esperando quorum
8. Workers limpian recursos y retornan
9. Thread principal hace join()
10. Finaliza el servidor con codigo 0

El handler no ejecuta los join directamente, request_shutdown() solamente marca la cancelación y cierra el listener. La limpieza completa pasa en el finally del run(), acto seguido los sockets y tempdirs se cierran mediante context managers.

Por otro lado, el shutdown del cliente ejecuta el siguiente flujo:

1. Se cancela el context
2. Se cierra la conexión
3. Las operaciones de red como read/recv retornan error
4. run() termina
5. defer cierra sockets y archivos
6. elimina output temporal
7. Exit con codigo 0

El cliente escribe en un archivo temporal y solo lo renombra cuando se completa el protocolo. Si antes de que pase eso llega el SIGTERM, se cierra y elimina el temporal. 

# Justificación de librerias más importantes elegidas

## Librerias del servidor

- Threading: Las operaciones de I/O bound hacen que el uso de esta libreria sea muy beneficioso. El trabajo se basa en esperar conexiones (thread principal), recibir y enviar data por sockets (los threads no-daemon). El GIL es el global interpreter lock, de esta forma CPython procesa en un mismo proceso un solo thread que ejecute bytecode python al mismo tiempo. Si bien es un tecnicismo que parece que es una pésima idea para threads, en el caso del tp0, no es grave. 
Para operaciones CPU bound, el GIL impide procesamiento paralelo y ahí si se nota más el problema. Calculos pesados no van a poder ejecutarse en paralelo sobre un cpu > 1 núcleo, porque un solo proceso va a generar que no se aprovechen ambos nucleos y que todo se ejecute concurrentemente cuando podria ser paralelo.

Para nuestro caso, muchas veces los threads se encuentran bloqueados esperando: accept(), recv(), send(), leer archivos y el condition.wait_for(). En esos momentos, el thread esta en estado SLEEPING y no consume cpu, por lo que no es significativo el bloqueo del paralelismo en single-process. 
Así es como el GIL se libera durante operaciones I/O.

- tempfile: Aquí se usa TemporaryDirectory otorga almacenamiento aislado por conexión. También permite que al finalizar la sesión, incluso ante errores o shutdown, se puedan desechar los archivos temporales.

## Librerias del cliente

- encoding/binary: Se utiliza unicamente para convertir enteros de tamaño fijo hacia y desde big-endian. no define ni resuelve el protocolo completo..
La estructura, los campos y las longitudes del protocolo se implementaron manualmente.

- context: Permite propagar el error, pudiendo hacer una cancelación cooperativa durante una conexión, lectura de archivo y comunicación con el servidor. Esto ayuda mucho a hacer una salida graceful ante SIGTERM.

- bufio: En el trabajo práctico se lee el archivo incrementalmente mediante buffer reutilizable. La lectura del archivo se realiza mediante esta librería.

- encoding/csv: se utiliza para escribir el archivo csv de ganadores. No
participa en la serialización del protocolo de red.




