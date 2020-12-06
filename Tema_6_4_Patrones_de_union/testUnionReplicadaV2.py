import unittest
import re
from unionReplicadaV2 import unionReplicadaV2

class TestReplicatedJoin(unittest.TestCase):
        
    def setUp(self):
        #Archivo para la unión 
        self.archivo = "archivos_datos/tablaB.csv"
    def test_rj(self):
        
        #mr_job = unionReplicadaV2(['--runner=inline',self.archivo])
        mr_job = unionReplicadaV2(['--runner=inline'])
        with mr_job.make_runner() as runner: #Ejecuto el MRJob
            runner.run()
            for key,value in mr_job.parse_output(runner.cat_output()):
                print (key,value)
        
if __name__ == '__main__':
    unittest.main()
