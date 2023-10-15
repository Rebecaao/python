import os
import time
import re
import mysql.connector as mysql

PATH = 'C:\\Users\\ricas\\Documents\\testethales'
ICCIDS = []

def connect_to_db():
  '''
    Connects to the Database and returns the connection
  '''
  return mysql.connect(host="localhost", user="root", password="1234", database="thales")

def insert_into_database(query, params):
  '''
    Inserts a single value to the database and return the inserted ID

    query: Query to be executed
    params: Tuple ou list with the parameters expected by the query
  '''
  database = connect_to_db()
  cursor = database.cursor()     
  
  cursor.execute(query, params)
  database.commit()
  
  database.close()
  return cursor.lastrowid

def insert_multiple_values(query, params, return_query=''):
  '''
    Inserts multiple rows for better performance and return all the inserted rows

    query: Query to be executed
    params: List with the expected paramaters for all rows
    return_query: Query to retrieve the inserted values
  '''
  database = connect_to_db()
  cursor = database.cursor()     
  results = []
  
  cursor.executemany(query, list(params))
  database.commit()

  if return_query != '':
    cursor.execute(return_query)
    results = cursor.fetchall()
  
  database.close()
  
  return [row[0] for row in results]

def read_file(filename):
  '''
    Reads a file and return the orders info and ICCIDs
    
    filename: Full file name to be read
  '''
  info_regex = '^(.{1,}): (.{1,})$'
  iccid_regex = '\d{1,}$'

  archive_info = {}

  with open(filename, 'r') as file:
    for line in file.readlines():
      treated_line = line.rstrip('\n').strip()
      info_match = re.match(info_regex, treated_line)

      if info_match:
        # Since theres two repeteaded keys called Package this code checks if this key exists and then adds a 2 to the name
        if info_match.group(1) in archive_info.keys():
          archive_info[f'{info_match.group(1)}2'] = info_match.group(2)
        else:
          archive_info[info_match.group(1)] = info_match.group(2)
      elif re.match(iccid_regex, treated_line):
        ICCIDS.append(treated_line)

  return archive_info

def insert_collection(tableName, archive_id):
  '''
    Inserts a collection with the given table name and returns the created ID

    tableName: Name of the collection table to be inserted
  '''
  query = '''
    INSERT INTO TABLE ( ID_Archive_key, ID_Situation_key,ID_Reading_key)
    VALUES (%s,%s,%s);
  '''.replace('TABLE', tableName)

  return insert_into_database(query, (archive_id, 1, 1))

def insert_values(interval, query, fixed_values, return_query, parent=None):
  '''
    Inserts each Outerbox, InnerBox and Bag creating the necessary relation between them

    interval: Amount of ICCIDs that each category contains
    query: Query to insert the values on the category table
    fixed_values: Fixed parameters to insert in the category table
    return_query: Query to return the inserted values
    parent: Object that contains the necessary information from the parent 
      interval: Amount of ICCIDs that the parent contains
      column_name: Name of the ID column of the parent 
      values: Values of the parent IDs
  '''
  values_to_insert = []
  parent_control, parent_index = (0, 0)

  for row in range(0, len(ICCIDS), interval):
    data = {
      'Starting_quantity': row + 1,
      'End_Quantity': row + interval,
      'Initial_iccid': ICCIDS[row],
      'End_iccid': ICCIDS[row + interval - 1]
    }

    if parent is not None:
      if parent_control == parent['interval']:
        parent_control = 0
        parent_index += 1

      data[parent['column_name']] = parent['values'][parent_index]
      
    parent_control += interval

    data.update(fixed_values)
    values_to_insert.append(list(data.values()))
    
  return insert_multiple_values(query, values_to_insert, return_query)

def main():
  '''
    Executes the main program that reads the file and inserts the Archive info, OuterBoxes, InnerBoxes, Bags, Kits and necessary collections
  '''
  for file in os.listdir(PATH):
    # Checks if the item is a file and if is TXT type
    if os.path.isfile(os.path.join(PATH, file)) and '.txt' in file:
      global ICCIDS

      start = time.time()
      archive_info = read_file(os.path.join(PATH, file))
      print(f'Exportando o arquivo {file} para o Banco de dados com {len(ICCIDS)} ICCIDs')
      print('Tempo para ler arquivo: {:.2f}s'.format(time.time() - start))

      # Inserindo as informações do arquivo no banco
      start = time.time()
      archive_info['ID_Situation_key'] = 1
      query = '''
        INSERT INTO TblArchive 
        (Item_Code,Item_Description,Customer,Provider,Packaging,CUSTOMER_PO,Batch,QUANTITY,HLR,EAN,Packaging_2,Profile,ID_Situation_key)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
      '''
      
      archive_id = insert_into_database(query, list(archive_info.values()))
      print('Tempo para inserir as informações do arquivo: {:.2f}s'.format(time.time() - start))
      
      # Inserindo OuterBoxes
      start = time.time()
      collection_id = insert_collection('tblCollectionOuterBox', archive_id)

      query = '''
        INSERT INTO tblOuterBox 
        (Starting_quantity, End_Quantity, Initial_iccid, End_iccid, ID_CollectionOuterBox_key, ID_SituationInner_key, ID_Situation_key) 
        VALUES (%s,%s,%s,%s,%s,%s,%s)
      '''
      return_query = f'''select ID_OuterBox from tblouterbox WHERE ID_CollectionOuterBox_Key = {collection_id}'''

      fixed_values = {'ID_CollectionOuterBox_key': collection_id, 'ID_SituationInner_key': 1, 'ID_Situation_key': 1 }

      outerboxes = insert_values(interval=1000, query=query, fixed_values=fixed_values, return_query=return_query)
      print('Tempo para inserir Outerboxes: {:.2f}s'.format(time.time() - start))

      # Inserindo InnerBoxes
      start = time.time()
      collection_id = insert_collection('tblCollectionInnerBox', archive_id)

      query = '''
        INSERT INTO tblInnerBox 
        (Starting_quantity, End_Quantity, Initial_iccid, End_iccid, ID_OuterBox_key, ID_CollectionInnerBox_key, ID_SituationKit_key, ID_Situation_key) 
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
      '''
      return_query = f'''select ID_InnerBox from tblInnerbox WHERE ID_CollectionInnerBox_key = {collection_id}'''

      fixed_values = {'ID_CollectionOuterBox_key': collection_id, 'ID_SituationInner_key': 1, 'ID_Situation_key': 1 }

      innerboxes = insert_values(
        interval=100,
        query=query,
        fixed_values=fixed_values,
        parent={'values': outerboxes, 'interval': 1000, 'column_name': 'ID_OuterBox_key'},
        return_query=return_query
      )
      print('Tempo para inserir Innerboxes: {:.2f}s'.format(time.time() - start))
      
      # Inserindo Bags
      start = time.time()
      query = '''
        INSERT INTO tblBagTen 
        (Starting_quantity, End_Quantity, Initial_iccid, End_iccid, ID_InnerBox_key, ID_Archive_key, ID_SituationKit_key, ID_Situation_key) 
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
      '''
      return_query = f'''select ID_BagTen from tblBagTen WHERE ID_Archive_key = {archive_id}'''

      fixed_values = {'ID_Archive_key': archive_id, 'ID_SituationInner_key': 1, 'ID_Situation_key': 1 }

      bags = insert_values(
        interval=10,
        query=query,
        fixed_values=fixed_values,
        parent={'values': innerboxes, 'interval': 100, 'column_name':'ID_InnerBox_key'},
        return_query=return_query
      )
      print('Tempo para inserir Bags: {:.2f}s'.format(time.time() - start))

      # Inserindo Kits
      start = time.time()
      query = '''
        INSERT INTO tblKit 
        (Iccid, ID_Archive_key, ID_CollectionKit_key, ID_Situation_key, ID_BagTen_key, ID_InnerBox_key, ID_OuterBox_key) 
        VALUES (%s,%s,%s,%s,%s,%s,%s)'''
      
      collection_id = insert_collection('tblCollectionKit', archive_id)
      
      bag_control, innerbox_control, outerbox_control = (0, 0, 0)
      bag_index, innerbox_index, outerbox_index = (0, 0, 0)
      bag_interval, innerbox_interval, outerbox_interval = (10, 100, 1000)
      values_to_insert = []
      
      for row in range(0, len(ICCIDS)):
        if (bag_control == bag_interval):
          bag_index +=1
          bag_control = 0
        
        if (innerbox_control == innerbox_interval):  
          collection_id = insert_collection('tblCollectionKit', archive_id)    
          innerbox_index += 1
          innerbox_control = 0 
        
        if (outerbox_control == outerbox_interval):
          outerbox_index += 1
          outerbox_control = 0


        data = {
          'Iccid': ICCIDS[row],
          'ID_Archive_key': archive_id,
          'ID_CollectionKit_key': 1,
          'ID_Situation_key': 1,
          'ID_BagTen_key': bags[bag_index], 
          'ID_InnerBox_key': innerboxes[innerbox_index], 
          'ID_OuterBox_key': outerboxes[outerbox_index]
        }

        bag_control+=1
        innerbox_control+=1
        outerbox_control+=1
        
        values_to_insert.append(list(data.values()))
        
      insert_multiple_values(query, values_to_insert)
      print('Tempo para inserir Kits: {:.2f}s'.format(time.time() - start))

      start = time.time()
      os.rename(os.path.join(PATH, file), f'{PATH}/processados/{file}')

      ICCIDS = []

      print('Tempo para mover o arquivo: {:.2f}s'.format(time.time() - start))
      print(f'Arquvo {file} exportado com sucesso!')
      print('-'*10)

start = time.time()
main()
print('Tempo total: {:.2f}s'.format(time.time() - start))