'''	
@Author: Aishwarya
@Date: 2022-1-4
@Last MOdified:2022-1-4
@Title : Reading text,csv,json file 
'''
########################################################################################################
import subprocess
from hdfs.ext.kerberos import KerberosClient
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import hdfs3
from hdfs3 import HDFileSystem
hdfs_client = KerberosClient('http://localhost:9870') 
#hdfs = pa.hdfs.connect('localhost', port=54310)

def readcsv():
	try:
		with hdfs_client.read('/test/files/trees.csv') as reader: 
			df = pd.read_csv(reader, sep = ';', error_bad_lines = False)
			print(df)
		print("-------------------------------------------------------------")
	except Exception as e:
		print(e)

def readtext():
	try:
		cat = subprocess.Popen(["hadoop", "fs", "-cat", "/test/files/a.txt"], stdout=subprocess.PIPE)
		for line in cat.stdout:
	    		print (line)
		print("-------------------------------------------")
	except Exception as e:
		print(e)
def readjson():
	try:
		cat = subprocess.Popen(["hadoop", "fs", "-text", "/test/files/sample.json"], stdout=subprocess.PIPE)
		for line in cat.stdout:
	    		print (line)
		print("-------------------------------------------")
	except Exception as e:
		print(e)

if __name__ == "__main__":
	readcsv()
	readtext()
	readjson()
