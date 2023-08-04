# -*- coding:utf-8 -*-
from tinydb import TinyDB
from tinydb import where
from operator import itemgetter
import os
import time
import subprocess

class Database:
    def __init__(self, dbFile):
        self.db = TinyDB(dbFile)

    def insertdb(self, value1, value2, value3, value4, value5, value6, value7):
        self.db.insert({"name":value1, "snapshot":value2, 
            "snapshotRestore":value3, "image":value4, 
            "imageRestore":value5, "time":value6,
            "checksum":value7})

    def printdb(self):
        data = self.db.all()
        return data

    def truncatedb(self):
        self.db.truncate()
  
    def searchdb(self, field, value):
        data = self.db.search(where(field) == value)
        sortdata = sorted(data, key = itemgetter('time'), reverse=True)
        if sortdata:
            return sortdata[0]
        else:
            return

    def selectdb(self, field1, value1, field2, value2):
        data = self.db.search((where(field1) == value1)&(where(field2) == value2))
        if data:
            return data[0]
        else:
            return

    def updatedb(self, field, value, snapshot):
        res = self.db.update({field:value}, where("snapshot") == snapshot)
        if not res:
            return True
        else:
            return False

    # def changedb(self, snap_name):
    #     res = self.db.update({"path":""}, where("snapshot") == snap_name)
    #     if not res:
    #         return True
    #     else:
    #         return False

    def finddb(self, field, value):
        data = self.db.search(where(field) == value)
        sortdata = sorted(data, key = itemgetter('time'), reverse=False)
        if sortdata:
            return sortdata
        else:
            return

    def deletedb(self, snapshot):
        res = self.db.remove(where("snapshot") == snapshot)
        if res:
            return True
        else:
            return False  

'''
###测试代码
db_file = f'checksum.json'
db = Database(db_file)
db.truncatedb()
print("******************************insert********************************")
#[{'name': 'res_a', 'snapshot': 'resa320230313132918', 'snapshotRestore': 'resa320230313132918_r', 'image': 'resa320230313132918.img', 'imageRestore': '/dev/vg/lv01', 'time': '2023-03-13 13:29:18'}, {'name': 'res_b', 'snapshot': 'resa320230313133527', 'snapshotRestore': 'resa320230313133527_r', 'image': 'resa320230313133527.img', 'imageRestore': '/dev/vg/lv02', 'time': '2023-03-13 13:35:27'}]
db.insertdb('resa', 'resa320230313132918', '', '', '', '2023-03-13 13:29:18', 'f9c64b62cbb674b3d98dbb262f28b79f')
db.insertdb('resb', 'resb320230313133527', '', '', '', '2023-03-13 13:35:27', 'e5695303d8358bda85ea2b83dc686411')
db.insertdb('resa', 'resa320230313133718', '', '', '', '2023-03-13 13:37:18', 'bff10fea418278d6cd5fba6a2248fcb0')
db.insertdb('resa', 'resa320230313133718', '', '', '', '2023-03-13 13:37:18', 'bff10fea418278d6cd5fba6a2248fcb0')
print(db.printdb())
print("******************************findall********************************")
print(db.finddb("name", "resa"))

print("******************************update********************************")
db.updatedb("image", '/mnt/resa320230313132918.img', 'resa320230313132918')
db.updatedb("snapshotRestore", 'resa320230313132918_r', 'resa320230313132918')
db.updatedb("imageRestore", "lv/vg0/resa320230313132918_ir", 'resa320230313132918')
print(db.printdb())

print("******************************select********************************")
print(db.searchdb("name", "resa"))
print(db.selectdb("name", "resa", "snapshot", 'resa320230313132918'))

print("******************************delete********************************")
db.deletedb('resb320230313133527')
print(db.printdb())

print("******************************just test********************************")
print(db.finddb("name", "resb"))
'''





