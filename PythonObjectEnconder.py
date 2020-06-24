from json import JSONEncoder, dumps, loads
from io import BytesIO
import hashlib
import pickle
import gzip
from os import path


class PythonObjectEncoder(JSONEncoder):
    def __init__(self, partition):
        self.partition = partition
        self.keys = sorted(list(initial_partition.parts.keys()))
        self.objstrings = []
        self.hashstrings = []
        self.d_plan = dict.fromkeys(self.keys)
        
        
    def __call__(self, obj):
        if isinstance(obj, (list, dict, str, bytes, int, float, bool, type(None))):
            return JSONEncoder.default(self, obj)
        return {'_python_object': pickle.dumps(obj)}
    
    
    def update(self, partition):
        for i in range(len(self.keys)):
            self.d_plan[self.keys[i]] = list(partition.assignment.parts[self.keys[i]])
        encoded_plan = dumps(self.d_plan)
        compressed_plan = PythonObjectEncoder.compressStringToBytes(encoded_plan)
        hashstring = hashlib.md5(encoded_plan.encode('utf-8')).hexdigest()
        self.objstrings.append(compressed_plan)
        self.hashstrings.append(hashstring)
        
        
    def commit(self, file_name):
        final_table = dict(zip(self.hashstrings,self.objstrings))
        self.save_obj(final_table, file_name)
        
        
    def load_maps(self, file_name):
        decode_test = self.load_obj(file_name)
        recreated_maps = []
        for key in decode_test:
            recreated_maps.append(loads(PythonObjectEncoder.decompressBytesToString(decode_test[key])))
        return recreated_maps
        
    
    @staticmethod
    def compressStringToBytes(inputString):
        bio = BytesIO()
        bio.write(inputString.encode("utf-8"))
        bio.seek(0)
        stream = BytesIO()
        compressor = gzip.GzipFile(fileobj=stream, mode='w')
        while True:
            chunk = bio.read(8192)
            if not chunk:
                compressor.close()
                return stream.getvalue()
            compressor.write(chunk)

            
    @staticmethod
    def decompressBytesToString(inputBytes):
        bio = BytesIO()
        stream = BytesIO(inputBytes)
        decompressor = gzip.GzipFile(fileobj=stream, mode='r')
        while True:
            chunk = decompressor.read(8192)
            if not chunk:
                decompressor.close()
                bio.seek(0)
                return bio.read().decode("utf-8")
            bio.write(chunk)
        return None
    
    
    @staticmethod
    def save_obj(obj, name):
        if path.exists(name + '.pkl'):
            prev_map = PythonObjectEncoder.load_obj(name)
            final_map = {**prev_map, **obj}
            with open(name + '.pkl', 'rb+') as f:
                pickle.dump(final_map, f, pickle.HIGHEST_PROTOCOL)
        else:
            with open(name + '.pkl', 'wb') as f:
                pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)
        
        
    @staticmethod
    def load_obj(name):
        with open(name + '.pkl', 'rb') as f:
            return pickle.load(f)