# Importar modulo arcpy
import arcpy, os
import urllib, urllib2, json 
import time, os.path
import logging


# Parametros de entrada de las GDB y los directorios:
RutaGDBs = "\\\\1.1.194.141\\Giscorporativo\\arcgisserver\\replicas\\"
NombreGDB = "DV_REPLICADAS.gdb"
NombreGDB_old = "DV_REP_OLD.gdb"
VistaSDE = "AGUAPOTABLE@DBPRU.sde\\AGUAPOTABLE.SV_GIS_CATASTRO"
NombreVista = "SV_GIS_CATASTRO_REP"
fichero_log = "C:\\Mantenimiento\\Logs\\Replicar_SV_GIS_CATASTRO.log"
Indexes = "OBJECTID_1;SUPPLYID;COD_UNICOM;NIS_PADRE;NIS_RAD;COD_CLI;IND_USO_INTENS;NOM_CALLE;COD_LOCAL;NOM_LOCAL;COD_MUNIC;NOM_MUNIC;EST_SUM;COD_TAR;COD_CNAE;IND_MEDIDOR;TIP_FACT;IND_RECL_DISTR;CICLO;RUTA;NUM_ITIN;AOL_FIN;COD_SECTOR;NIV_SOCIO;IND_SERV_ALC;NUM_PUERTA;PARCELCODE;DUPLICADOR"

# Parametros de entrada de la conexion del server y los servicios que apuntan a esa gdb
# Se pone localhost porque al ejecutarse en la propia maquina del server no funciona con gisprd.sedapal.com
server = "localhost"
port = "6080" 
adminUser = "admin"  
adminPass = "G1Sc0n$0rc10"  
stopStart = "Stop"  
serviceList = "ConexDomSGC.MapServer;AguaPotable.MapServer;Demanda.MapServer;TematicosCE.MapServer;CatastroComercial.MapServer;UsuarioExterno.MapServer"


def gentoken(server, port, adminUser, adminPass, expiration=60):
    #Re-usable function to get a token required for Admin changes
    
    arcpy.AddMessage("generando token")
    query_dict = {'username':   adminUser,
                  'password':   adminPass,
                  'expiration': str(expiration),
                  'client':     'requestip'}
    
    query_string = urllib.urlencode(query_dict)
    url = "http://{}:{}/arcgis/admin/generateToken".format(server, port)
        
    token = json.loads(urllib.urlopen(url + "?f=json", query_string).read())
        
    if "token" not in token:
        logging.warning(token['messages'])
        logging.warning("error generando token")
        quit()
    else:
        return token['token']


def stopStartServices(server, port, adminUser, adminPass, stopStart, serviceList, token=None):  
    ''' Function to stop, start or delete a service.
    Requires Admin user/password, as well as server and port (necessary to construct token if one does not exist).
    stopStart = Stop|Start|Delete
    serviceList = List of services. A service must be in the <name>.<type> notation
    If a token exists, you can pass one in for use.  
    '''    
    
    # Get and set the token
    if token is None:       
        token = gentoken(server, port, adminUser, adminPass)
        logging.debug("token generado")
    
    # Getting services from tool validation creates a semicolon delimited list that needs to be broken up
    services = serviceList.split(';')
    
    #modify the services(s)    
    for service in services:        
        service = urllib.quote(service.encode('utf8'))        
        op_service_url = "http://{}:{}/arcgis/admin/services/{}/{}?token={}&f=json".format(server, port, service, stopStart, token)        
        status = urllib2.urlopen(op_service_url, ' ').read()
        
        if 'success' in status:
            logging.debug(str(service) + " === " + str(stopStart))
        else:
            logging.warning("No se ha podido " + str(stopStart) + " el servicio de mapa de :" + service)

    # Agregado para que tambien pare los servicios de impresion
    for service in services:        
        service = urllib.quote(service.encode('utf8'))        
        op_service_url = "http://{}:{}/arcgis/admin/services/Print/{}/{}?token={}&f=json".format(server, port, service, stopStart, token)        
        status = urllib2.urlopen(op_service_url, ' ').read()
        
        if 'success' in status:            
            logging.debug(str(service) + " === " + str(stopStart))
        else:            
            logging.warning("No se ha podido " + str(stopStart) + " el servicio print de : " + service)
    
    return
               
try:
    #Se configura el log
    logging.basicConfig(
         level=logging.DEBUG,
         format='%(asctime)s : %(levelname)s : %(message)s',
         filename = fichero_log,
         filemode = 'w')

    # Set workspace
    path = os.path.abspath(RutaGDBs)
    arcpy.env.workspace = RutaGDBs
    
    # Se paran los servicios que tiren de la capa SV_GIS_CATASTRO_REP
    logging.debug("Parando servicios...")
    stopStartServices(server, port, adminUser, adminPass, stopStart, serviceList)

    # Se agrega un tiempo adicional para que no haya bloqueos
    arcpy.AddMessage("Esperando: 120 segundos")
    logging.debug("Esperando: 120 segundos")
    time.sleep(120)

    # Se renombra la GDB
    try:
        logging.debug("Renombrando " + NombreGDB + " a " + NombreGDB_old)
        #arcpy.Rename_management(NombreGDB, NombreGDB_old)
        os.rename(RutaGDBs + NombreGDB, RutaGDBs + NombreGDB_old)
    except Exception, e:
        #Se ignora el error, ya que la renombra pese a saltar la excepcion
        logging.warning("Error Renombrando gdb " + str(e.args))

    # Se crea la nueva GDB solo si se ha conseguido renombrar
    if arcpy.Exists(NombreGDB):
        logging.warning("No se ha podido renombrar la gdb")
    else:
        #Solo se si ha conseguido renombrar se ejecuta el resto del script
        logging.debug("Se crea la nueva GDB")
        arcpy.CreateFileGDB_management(RutaGDBs, NombreGDB)

        # Copiando la nueva capa a la GDB como _NEW   
        logging.debug("Inicio Copia: de " + RutaGDBs + VistaSDE + " a " + RutaGDBs + NombreGDB + "\\"+ NombreVista)
        arcpy.Copy_management(RutaGDBs+VistaSDE, RutaGDBs+NombreGDB + "\\" + NombreVista, "FeatureClass") 
        logging.debug("Fin Copia")    
       
        # Si existe la gdb antigua se borra
        if arcpy.Exists(NombreGDB_old):
            logging.debug("Borrando " + NombreGDB_old)
            arcpy.Delete_management(NombreGDB_old)
    
        # Creando indices    
        indexArray = Indexes.split(";")
        for i in range(len(indexArray)):
            logging.debug("Index " + indexArray[i])
            arcpy.AddIndex_management(RutaGDBs+NombreGDB+"\\"+NombreVista, indexArray[i], "IDX" + str(i), "NON_UNIQUE", "NON_ASCENDING")
       
except Exception, e:
    # Si ocurre un error se devuelve el mensaje
    import traceback, sys
    logging.warning("No se ha podido actualizar la vista SV_GIS_CATASTRO: " + e.message)
        
finally:    
    # Start Service
    stopStart = "Start" 
    stopStartServices(server, port, adminUser, adminPass, stopStart, serviceList)
    logging.debug("Finalizada sincronizacion")
