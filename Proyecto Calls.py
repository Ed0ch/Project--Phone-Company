


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from math import factorial
from scipy import stats as st
import datetime as dt



# Carga los archivos de datos en diferentes DataFrames
df_calls=pd.read_csv('C:/Users/carlo/OneDrive/Documentos/Datasets Proyecto 2/megaline_calls.csv')
df_internet=pd.read_csv("C:/Users/carlo/OneDrive/Documentos/Datasets Proyecto 2/megaline_internet.csv")
df_messages=pd.read_csv("C:/Users/carlo/OneDrive/Documentos/Datasets Proyecto 2/megaline_messages.csv")
df_plans=pd.read_csv("C:/Users/carlo/OneDrive/Documentos/Datasets Proyecto 2/megaline_plans.csv")
df_users=pd.read_csv("C:/Users/carlo/OneDrive/Documentos/Datasets Proyecto 2/megaline_users.csv")


#Prepara los datos


# Imprimimos la información general/resumida sobre el DataFrame de las tarifas
df_plans.info() #Revisión del contenido del formato de nuestros datos
print(df_plans.head(10))
df_plans.duplicated().sum() #Contabilizar los datos que tenemos duplicados.
df_plans.sample(2) #Revisar una muestra de los planes para ver el contenido aleatorio de nuestros datos y revisar como se encuentra.


# Lo que pude observar es que no tenemos datos ausentes en la tarifas que nos plantean en ambos planes al mes, tampoco tenemos valores duplicado y lo que si pude observar es que tenemos la cantidad de megabytes incorrecta.
# Lo que si es que los megabytes por mes no están redondeados, pero como aun no hacemos ninguna operación no haré ninguna modificación a este nivel-

print(df_plans.head(2))

# Decidí no hacer modificaciones a este nivel




# Usuarios/as


# Imprimimos la información general/resumida sobre el DataFrame de usuarios

df_users.info() #Revisión del contenido de nuestros datos y su formato.
df_users['churn_date'].fillna(0) #Relleno de valores ausentes.
df_users.head(10)

df_users.sample() #Revisión aleatoria de losa tos de los usuarios.

print(df_users.isna().sum())
df_users.duplicated().sum()


# Lo que pude observar es que hay muchas lineas de usuarios que aun están activas, por lo que nos aparece un valor ausente y no tenemos valores duplicados,por lo que pensé a rellenarlas con la palabra "active" pero no sé si por el tipo de datos me vaya a generar un problema más adelante por lo que no haré modificaciones aun.

print(df_users.head(10))




#Llamadas


# Imprimimos la información general/resumida sobre el DataFrame de las llamadas

df_calls.info() #Revisión del formato de nuestros datos.

# Imprimimos una muestra de datos para las llamadas
df_calls.head(10)

# Lo que estaré corrigiendo es que tiene la duración en decimal en segundos por lo que voy a redondearla para trabajar con enteros en los minutos.


df_calls['duration']=df_calls['duration'].apply(np.ceil) #Redondear la suración de las llamadas
df_calls['duration']=df_calls['duration'].astype(int) #Convertir el tipo de numero a entero
print(df_calls.head(10)) #Imprime nuevamente las primeras columnas para revisar que los cambios estén hechos




# ## Mensajes


# Imprimimos la información general/resumida sobre el DataFrame de los mensajes.

df_messages.info() #Revisión de los datos de los mensajes.
print(df_messages.head(10))
print('cantidad de duplicados en df',df_messages.duplicated().sum()) #Revisamos que cantidad de mensajes está duplicada.
print('cantidad de ausentes en df',df_messages.isna().sum()) #Revisamos la cantidad de valores ausentes que tenemos en el Dataframe.

df_messages.sample(5) # Imprimmos  una muestra de datos para los mensajes

# En este punto pensé en que existirían llamadas repetidas pero viene siendo por el usuario con el identificador unico de la llamada, por lo que no hice ninguna modificación.

df_messages.isna().sum() #Revisión de valores ausentes.




#Internet

# Imprimimos la información general/resumida sobre el DataFrame de internet

df_internet.info() #Revisión de los datos del Internet.
df_internet.head(10) #Revisión de las primeras diez columnas.

# Imprimimos una muestra de datos para el tráfico de internet

df_internet.sample(5)


# En este caso se ven los megabytes aun con decimales pero como estaremos trabajando por mes, aun no realizarmeos su conversión a enteros



#Cálculos


# Número de llamadas hechas por cada usuario al mes.

df_calls['call_date']=pd.to_datetime(df_calls['call_date']) #Convertimos la columna del dataframe a formato de fecha.
df_calls['month']=df_calls['call_date'].dt.strftime('%m') #Creamos la columna de mes
df_calls.info()
df_calls_by_user=df_calls.groupby(['user_id','month']).size().reset_index(name='total_calls') #Creamos otra variable para agrupar por mes y por usuario el total de llamadas.
df_calls_by_user.head(10)




#Cantidad de minutos usados por cada usuario al mes. Guarda el resultado.
df_calls['call_date']=pd.to_datetime(df_calls['call_date'])#Convertimos la columna del dataframe a formato de fecha.
df_calls['month']=df_calls['call_date'].dt.strftime('%m')
df_minutes_total=df_calls.groupby(['user_id','month'])['duration'].sum().reset_index(name='total_duration') #Agrupamos en otra variable el usuario y el mes para poder sumar la duración total en minutos, dándole el nombre de duración total.
print(df_minutes_total.head(10)) #Mostramos las primeras diez lineas del dataframe ya calculado.




#Número de mensajes enviados por cada usuario al mes. Guarda el resultado.

df_messages['message_date']=pd.to_datetime(df_messages['message_date']) #Convertimos la columna a formato de fecha.
df_messages['month']=df_messages['message_date'].dt.strftime('%m') #Separamos el mes de cada una de las fechas.
df_messages_by_user=df_messages.groupby(['user_id','month']).size().reset_index(name='total_messages') #Agrupamos por usuario y mes para poder contabilizar el total de mensajes.
df_messages_by_user.head(10)




#Volumen del tráfico de Internet usado por cada usuario al mes. Guarda el resultado.

df_internet['session_date']=pd.to_datetime(df_internet['session_date']) #Convertimos la columna de sesión en formato de fecha.
df_internet['month']=df_internet['session_date'].dt.strftime('%m') #Separamos el mes.
df_internet_by_user=df_internet.groupby(['user_id','month'])['mb_used'].sum().reset_index(name='total_mb') #Creamos otra variable en la que agrupamos por usuario y mes para poder contabilizar el total de megabytes utilizados.
print(df_internet_by_user.head(10))


# Fusionamos los datos de llamadas, minutos, mensajes e Internet con base en user_id y month

df_junto=pd.concat([df_calls_by_user,df_minutes_total['total_duration'],df_messages_by_user['total_messages'],df_internet_by_user['total_mb']],axis=1) #Creamos otro dataframepara poder unir el total de  las llamadas, los mensajes y los megabytesen un sólo dataframe. 
print(df_junto.head()) #Imprimimos el dataframe.


# Añadimos la información de la tarifa
df_plan=df_junto.merge(df_users[['user_id','plan']],on='user_id',how='left') #Utilizamos la función merge para agregar la información de las tarifas.
df_plan.head()



# Calculos  del ingreso mensual para cada usuario

df_plan_m=df_plan.merge(df_plans,left_on='plan',right_on='plan_name',how='left') #Creamos otra variable para poder realizar los calculos de las tarifas.

df_plan_m['minutes_over']=(df_plan_m['total_duration']-df_plan_m['minutes_included']).clip(lower=0) #Calculamos cuantos minutos nos pasamos de los incluidos en el plan.
df_plan_m['messages_over']=(df_plan_m['total_messages']-df_plan_m['messages_included']).clip(lower=0) #Calculamos cuantos mensajes nos pasamos de los incluidos en el plan.
df_plan_m['gb_over']=np.ceil((df_plan_m['total_mb']-df_plan_m['mb_per_month_included']).clip(lower=0)/1024) #Calculamos cuantos megabytes nos pasamos de los incluidos en el plan y los convertimos a gigabyte.

df_plan_m['cost_extra_minutes']=df_plan_m['minutes_over']*df_plan_m['usd_per_minute'] #Multiplicamos el precio que se cobra por cada minuto extra del plan.
df_plan_m['cost_extra_messages']=df_plan_m['messages_over']*df_plan_m['usd_per_message'] #Multiplicamos el precio que se cobra por cada mensaje extra del plan.
df_plan_m['cost_extra_gb']=df_plan_m['gb_over']*df_plan_m['usd_per_gb'] #Multiplicamos el precio que se cobra por cada gigabyte extra utilizado en nuestro plan.

df_plan_m['monthly_cost']=df_plan_m['usd_monthly_pay']+df_plan_m['cost_extra_minutes']+df_plan_m['cost_extra_messages']+df_plan_m['cost_extra_gb'] #Calculamos el total al mes de cada uno de los servicios del plan para poder obtener el monto final.
df_plan_total=df_plan_m[['user_id','month','plan_name','monthly_cost']] #Creamos otra variable para asignar los totales.

print(df_plan_total.head()) #Imprimimos nuestro dataframe con las operaciones ya realizadas.




#En esta parte estaremos calculando algunas estadísticas descriptivas para poder estudiar el comportamiento del usuario.

### Llamadas



surf=df_plan[df_plan['plan']=='surf'].groupby(by='month')['total_duration'].mean() #Creamos una variable que nos filtre por el plan surf y nos agrupe por el promedio mensual de su total duración.
ultimate=df_plan[df_plan['plan']=='ultimate'].groupby(by='month')['total_duration'].mean() #Creamos una variable que nos filtre por el plan ultimate y nos agrupe por el promedio mensual de su total duración.

df_grafico=pd.concat([surf,ultimate],axis=1) #Creamos una variable para juntar nuestros dataframes.
df_grafico.plot(kind='bar') #Le indicamos que lo queremos tipo barra.
plt.legend(['Surf','Ultimate']) #Le indicamos que queremos que nos distinga la leyenda de cada uno de los planes.
plt.show()


# Comparamos el número de minutos mensuales que necesitan los usuarios de cada plan.
surf=df_plan[df_plan['plan']=='surf'].groupby(by='month')['total_duration'].sum() #Creamos una variable que nos filtre por el plan surf y nos agrupe por el total  mensual de su total duración.
ultimate=df_plan[df_plan['plan']=='ultimate'].groupby(by='month')['total_duration'].sum() #Creamos una variable que nos filtre por el plan ultimate y nos agrupe por el total  mensual de su total duración.
df_grafico=pd.concat([surf,ultimate],axis=1)#Unimos nuestras variables.

df_grafico.plot(kind='hist',bins=10,alpha=0.80) #Cremos un histograma 
plt.legend(['Surf','Ultimate'])


# Calculamos la media y la varianza de la duración mensual de llamadas.

print('la media es igual a',df_plan.groupby('plan')['total_duration'].mean()) #Calculamos la media, se agrupa por plan y sacamos la media de la duración total.
print('la varianza es igual a',df_plan.groupby('plan')['total_duration'].var())

media_calls=df_plan['total_duration'].mean()
varianza_calls=np.var(df_plan['total_duration'])


# Trazamos un diagrama de caja para visualizar la distribución de la duración mensual de llamadas

sns.boxplot(x='total_duration',data=df_plan)

surf=df_plan[df_plan['plan']=='surf'][['month','total_duration']].reset_index(drop='True')
ultimate=df_plan[df_plan['plan']=='ultimate'][['month','total_duration']].reset_index(drop='True')
df_box=pd.concat([surf,ultimate],axis=1,keys=['surf','ultimate'])

sns.boxplot(data=df_box)



# Lo que puedo observar es que la media de la variable tiende a ser 435 minutos por mes en ambos planes, por lo que podemos
# observar un comportamiento similar para ambos planes.
# Viendo las gráficas de manera separada podemos ver que en el mes de Febrero el plan ultimate tuvo un pico.
# También se pueden ver algunos valores atípicos dentro del diagrama de caja de bigotes para ambos planes.



#Mensajes




# Compramos el número de mensajes que tienden a enviar cada mes los usuarios de cada plan
surf=df_plan[df_plan['plan']=='surf'].groupby(by='month')['total_messages'].mean()
ultimate=df_plan[df_plan['plan']=='ultimate'].groupby(by='month')['total_messages'].mean()
df_grafico=pd.concat([surf,ultimate],axis=1)


#Creamos una gráfica de barras.
df_grafico.plot(kind='bar',title='Número promedio de mensajes por mes por plan')
plt.legend(['surf','ultimate'])

surf=df_plan[df_plan['plan']=='surf'].groupby(by='month')['total_messages'].sum()
ultimate=df_plan[df_plan['plan']=='ultimate'].groupby(by='month')['total_messages'].sum()
df_grafico=pd.concat([surf,ultimate],axis=1)

#Creamos un histograma para ver la frecuencia de los mensajes.
df_grafico.plot(kind='hist',bins=10,alpha=0.8)
plt.legend(['Surf','Ultimate'])




#Calculamos la media y la varianza 
print('la media es igual a',df_plan['total_messages'].mean())
print('la varianza es igual a',np.var(df_plan['total_messages']))

media=df_plan['total_messages'].mean()
varianza=np.var(df_plan['total_messages'])


#Graficamos diagramas de caja de bigotes
surf=df_plan[df_plan['plan']=='surf'][['month','total_messages']].reset_index(drop='True')
ultimate=df_plan[df_plan['plan']=='ultimate'][['month','total_messages']].reset_index(drop='True')
df_box=pd.concat([surf,ultimate],axis=1,keys=['surf','ultimate'])
sns.boxplot(data=df_box)



# Se puede osbervar en una de las gráficas que los usuarios del plan sur suelen mandar más mensajes en promedio por mes que los del plan ultimate a pesar de que el excedente de mensajes es más caro, se suele ver una diferencia considerable en los mensajes.


#Internet



#Creamos un gráfico que nos muestre el numero promedio de Gb por mes en cada plan
surf=df_plan[df_plan['plan']=='surf'].groupby(by='month')['total_mb'].mean()/1024
ultimate=df_plan[df_plan['plan']=='ultimate'].groupby(by='month')['total_mb'].mean()/1024
df_grafico=pd.concat([surf,ultimate],axis=1)
df_grafico.plot(kind='bar',title='Número promedio de gb por mes por plan')
plt.legend(['surf','ultimate'])



#Creamos un histograma que nos muestre la distribución de la cantidad de Gb
surf=df_plan[df_plan['plan']=='surf'].groupby(by='month')['total_mb'].sum()/1024
ultimate=df_plan[df_plan['plan']=='ultimate'].groupby(by='month')['total_mb'].sum()/1024
df_grafico=pd.concat([surf,ultimate],axis=1)
df_grafico.plot(kind='hist',bins=10,alpha=0.8)
plt.legend(['Surf','Ultimate'])


#Creamos un gráfico de caja de bigotes
df_plan['gb_per_month']=df_plan['total_mb']/1024
surf=df_plan[df_plan['plan']=='surf'][['month','gb_per_month']].reset_index(drop='True')
ultimate=df_plan[df_plan['plan']=='ultimate'][['month','gb_per_month']].reset_index(drop='True')
df_box=pd.concat([surf,ultimate],axis=1,keys=['surf','ultimate'])
sns.boxplot(data=df_box)

#Obtenemos la media y la varianza de los Gb.
print('La media es igual a',df_plan['gb_per_month'].mean())
print('La varianza es igual a',np.var(df_plan['gb_per_month']))

media=df_plan['gb_per_month'].mean()
varianza=np.var(df_plan['gb_per_month'])




# Aqui es mucho más evidente de que a pesar de que el internet se mide en Gb, la cantidad de volumen de los usuarios del plan ultimate es mayor en la mayoría de  los meses, lo que refleja por ende un consumo mayor, lo que hace concordancia a su paquete que incluye una mayor cantidad de Gb que el de surf.



#Ingreso


#Creamos un gráfico que nos muestre el ingreso mensual de cada uno de los planes
surf=df_plan_total[df_plan_total['plan_name']=='surf'].groupby(by='month')['monthly_cost'].mean()
ultimate=df_plan_total[df_plan_total['plan_name']=='ultimate'].groupby(by='month')['monthly_cost'].mean()

df_grafico=pd.concat([surf,ultimate],axis=1)
df_grafico.plot(kind='bar')
plt.legend(['Surf','Ultimate'])
plt.show()

#Creamos un histograma que nos muestre la distribución del ingreso de cada uno de los planes


surf=df_plan_total[df_plan_total['plan_name']=='surf'].groupby(by='month')['monthly_cost'].sum()
ultimate=df_plan_total[df_plan_total['plan_name']=='ultimate'].groupby(by='month')['monthly_cost'].sum()
df_grafico=pd.concat([surf,ultimate],axis=1)
df_grafico.plot(kind='hist',bins=8,alpha=0.8)
plt.legend(['Surf','Ultimate'])




#Mostramos la distribución mediante un diagrama de cada y bigotes.
sns.boxplot(x='monthly_cost',data=df_plan_total)


#Calculamos la media mensual de cada uno de los planes
media_surf=df_plan_total[df_plan_total['plan_name']=='surf'].groupby('month')['monthly_cost'].mean()
media_ultimate=df_plan_total[df_plan_total['plan_name']=='ultimate'].groupby('month')['monthly_cost'].mean()
varianza=np.var(df_plan_total['monthly_cost'])

print('La media de surf es:',media_surf)
print('La media de surf es:',media_ultimate)




# Es notable como el ingreso de un plan más caro como el de ultimate genera mucho más ganancia que el otro a pesar de que es más caro, ya que si nos fijamos en la grafica, se cobra mucho más por el plan ultimate en todos los meses a diferencia del surf.

#Pruebas de hipótesis


#Hay que probar que la hipotesis nula de las medias sean distintas
alpha=0.05

results=st.ttest_ind(media_surf,media_ultimate,equal_var='False')

print('valor p:',results.pvalue)

if results.pvalue<alpha:
    print('Rechazamos la hipótesis nula')
    
else:
    print('No podemos rechazar la hipótesis nula')


# Por lo que se concluye de que las medias no son iguales debido a que el valor de p está muy alejado de 1, por lo que podemos concluir que las medias son diferentes.




#Creación de df para poder operar la region de Ny y compararlo con las otras regiones
df_Ny=df_users[['user_id','city']]
df_ny=df_plan_total.merge(df_Ny[['user_id','city']],on='user_id',how='left')

df_nyj=df_ny[df_ny['city']=='New York-Newark-Jersey City, NY-NJ-PA MSA'] #Obtenemos el df para pode interactuar con la ciudad específica

df_others=df_ny[df_ny['city']!='New York-Newark-Jersey City, NY-NJ-PA MSA'] #Obetenemos el df para poder interacutar con las otras ciudades

media_ny=df_nyj.groupby('month')['monthly_cost'].mean() #sacamos el set de medias para la prueba
media_others=df_others.groupby('month')['monthly_cost'].mean() 

media_ny.head(12)





#Prueba de hipótesis de la región de Ny

alpha=0.05

results=st.ttest_ind(media_ny,media_others)

print('valor p:',results.pvalue)

if results.pvalue<alpha:
    
    print('Rechazamos la hipótesis nula')
    
else:
    print('No podemos rechazar la hipótesis nula')
    


# No se puede rechazar la hipotesis nula por lo que concluimos que no hay suficiente evidencia para afirmar que las medias son iguales entre Ny y las demás ciudades


# ## Conclusión general

# Para concluir a pesar de que al inicio tenía la suposición de que al tener un plan más barato atraría un mayor volumen de personal y por ende nos traería una ganancia mucho mayor, através del análisis me di cuenta de que no es así.
# Pude observar elcomportamiento de los datos y concluir que las medias son diferentes significativamente.
# Me gustó el hecho de comprobar que los ingresos marcan la diferencia a pesar del costo del plan que escogiese.
# 
