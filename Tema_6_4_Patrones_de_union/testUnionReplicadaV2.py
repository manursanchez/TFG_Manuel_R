import unittest
import re
from unionReplicadaV2 import unionReplicadaV2

class TestReplicatedJoin(unittest.TestCase):
        
    def setUp(self):
        #Archivo para la unión 
        self.archivo = "archivos_datos/tablaB.csv"
        self.archivo2="archivos_datos/tablaA.csv"
    def test_rj(self):
        #self.archivo2=json.dumps(self.diccionario)
        #Mando al objeto MRJob el archivo a unir 
        #con el diccionario creado en la clase
        mr_job = unionReplicadaV2(['--runner=inline',self.archivo])
        with mr_job.make_runner() as runner: #Ejecuto el MRJob
            runner.run()
            for key,value in mr_job.parse_output(runner.cat_output()):
                print (key,value)
                
        mr_job=unionReplicadaV2(['--runner=inline',self.archivo2])
        with mr_job.make_runner() as runner: #Ejecuto el MRJob
            runner.run()
            for key,value in mr_job.parse_output(runner.cat_output()):
                print (key,value) 
        
if __name__ == '__main__':
    unittest.main()
