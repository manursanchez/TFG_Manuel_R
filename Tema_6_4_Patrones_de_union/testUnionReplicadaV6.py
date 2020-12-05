import unittest
import re,json
from unionReplicadaV6 import unionReplicadaV6

class TestReplicatedJoin(unittest.TestCase):
        
    def setUp(self):
        #Archivo para la unión
        self.archivo = "archivos_datos/tablaB.csv"
        
        """#self.archivo="clientes.csv"
        self.archivo = "ventas.csv"
        self.diccionario={}
        with open(self.archivo) as f:
            self.tablaEnMemoria = set(line.strip() for line in f)
        for linea in self.tablaEnMemoria:
            encontrado=re.search('[a-zA-Z]',linea[0])#Para que no tenga en cuenta las cabeceras de las tablas
            if encontrado==None:
                datos = linea.split(";")
                self.diccionario[datos[0]]=datos"""
        
        #print ("Diccionario relleno: ",self.diccionario) #Veo si el diccionario está relleno
        
    def test_rj(self):
        #self.archivo2=json.dumps(self.diccionario)
        mr_job = unionReplicadaV6(['--runner=inline',self.archivo])#Mando al objeto 
                                                                   #MRJOB el diccionario.
                                                                    #ERROR. Si pongo un
                                                                    #archivo, sí funciona.
        with mr_job.make_runner() as runner: #Ejecuto el MRJob
            runner.run()
            for key,value in mr_job.parse_output(runner.cat_output()):
                print (key,value) 
            
if __name__ == '__main__':
    unittest.main()
