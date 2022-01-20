'''	
@Author: Aishwarya
@Date: 2022-1-4
@Last MOdified:2022-1-4
@Title : Reading avro file 
'''
########################################################################################################
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader #DatumWriterschema = avro.schema.parse(open("user.avsc").read())writer = DataFileWriter(open("users.avro", "wb"), DatumWriter(), schema)

reader = DataFileReader(open("/home/hadoop/Files/data.avro", "rb"), DatumReader())
for user in reader:
    print(user)
    print('===================')
reader.close()

