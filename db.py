from tinydb import TinyDB
from tinydb import where
from operator import itemgetter
import os
import time
import subprocess
from prettytable import PrettyTable
import execute as e

class Database:
  def __init__(self, dbFile):
    self.db = TinyDB(dbFile)

  def insertdb(self, value1, value2, value3, value4, value5):
    self.db.insert({"resource":value1, "snapshot":value2, "snap_md5":value3, "time":value4, "path":value5})

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

  def updatedb(self, snap_name, file_path):
    self.db.update({"path":file_path}, where("snapshot") == snap_name)

  def changedb(self, snap_name):
    res = self.db.update({"path":""}, where("snapshot") == snap_name)
    if not res:
        return True
    else:
        return False

  def finddb(self, field, value):
    data = self.db.search(where(field) == value)
    sortdata = sorted(data, key = itemgetter('time'), reverse=False)
    if sortdata:
        return sortdata
    else:
        return

  def deletedb(self, snap_name):
    res = self.db.remove(where("snapshot") == snap_name)
    if res:
        return True
    else:
        return False  

###测试代码
