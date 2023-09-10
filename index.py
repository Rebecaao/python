import os
import subprocess
import smtplib
from email.message import EmailMessage
from datetime import datetime
from os import listdir
from os.path import isfile, join
import re


path = 'C:\\Users\\ricas\Documents\\testethales';
os.chdir(path)
import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="1234",
  database="db_teste"
)


def setinsert(lst):
    mycursor = mydb.cursor()     
    sql = "insert into login2 (iccid) values (%s);"
    # valy = [(val)]
    mycursor.executemany('insert into login2 (iccid) values(%s)', lst)
    # mycursor.execute(sql, valy)
    mydb.commit()
    print(mycursor.rowcount, "record inserted.")


def readArchive():
 cont = 0
 for fileTXT in os.listdir():
    lst = []

    with open(fileTXT) as ref_arquivo:
      for linha in ref_arquivo.readlines():
        if re.match('[0-9]{20}', linha):
          lst.append((linha.rstrip('\n'),))
    # cont = cont+1  
    setinsert(lst)
    # for nome in linha:
    #  quantlin = len(linha) 
    #  nome = nome.rstrip('\n')


readArchive()