import mysql.connector as mysql
import time
import os


path = 'C:\\Users\\ricas\\Documents\\testethales'

def setinsert(query, params):
  database = mysql.connect(
    host="localhost",
    user="root",
    password="1234",
    database="thales"
  )

  mycursor = database.cursor()     
  mycursor.execute(query, params)
  database.commit()
  
  return mycursor.lastrowid

iccds = []
values = []

with open(f'{path}/TA080240.txt') as file:
  for index, row in enumerate(file.readlines()):
    if index < 12:
      values.append(row.split(':')[1].strip())

    if index >= 14:
      if row != '\n':
        iccds.append(row.replace('\n', ''))

start = time.time()

archive_query = '''
  INSERT INTO TblArchive (Item_Code,Item_Description,Customer, Provider, Packaging, CUSTOMER_PO,Batch,  QUANTITY, HLR, EAN, Packaging_2, Profile,  ID_Situation_key )
  values (%s,%s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s);
'''

archive_id = setinsert(archive_query, (values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7], values[8], values[9], values[10], values[11], 1))

interval = 1000

collection_query = '''
  insert into tblCollectionOuterBox ( ID_Archive_key, ID_Situation_key,ID_Reading_key)
  values (%s,%s,%s);
'''
collection_id = setinsert(collection_query, (archive_id, 1, 1))

outter_box_query = '''
  insert into tblOuterBox 
  (Starting_quantity, End_Quantity, Initial_iccid, End_iccid, ID_CollectionOuterBox_key, ID_SituationInner_key, ID_Situation_key) 
  values (%s,%s,%s,%s,%s,%s,%s)'''

outerboxes_ids = []

for row in range(0, len(iccds), interval):
  id = setinsert(outter_box_query, (row + 1, row + interval, iccds[row], iccds[row + interval - 1], collection_id, 1, 1))
  outerboxes_ids.append(id)

collection_query = '''
  insert into tblCollectionInnerBox ( ID_Archive_key, ID_Situation_key,ID_Reading_key)
  values (%s,%s,%s);
'''
inner_collection_id = setinsert(collection_query, (archive_id, 1, 1))

interval = 100
box_interval = 1000
a = 0
x = 0


inner_box_query = '''
  insert into tblInnerBox 
  (Starting_quantity, End_Quantity, Initial_iccid, End_iccid, ID_OuterBox_key, ID_CollectionInnerBox_key, ID_SituationKit_key, ID_Situation_key) 
  values (%s,%s,%s,%s,%s,%s,%s,%s)'''

innerboxs_ids = []

for row in range(0, len(iccds), interval):
  id = setinsert(inner_box_query, (row + 1, row + interval, iccds[row], iccds[row + interval - 1], outerboxes_ids[x], inner_collection_id, 1, 1))
  innerboxs_ids.append(id)
  
  a+=interval

  if (a == box_interval):
    x +=1
    a = 0

interval = 10
inner_box_interval = 100
a = 0
x = 0

bag_box_query = '''
  insert into tblBagTen 
  (Starting_quantity, End_Quantity, Initial_iccid, End_iccid, ID_Archive_key, ID_InnerBox_key, ID_SituationKit_key, ID_Situation_key) 
  values (%s,%s,%s,%s,%s,%s,%s,%s)'''

bags_ids = []

for row in range(0, len(iccds), interval):
  if (a == inner_box_interval):
    x +=1
    a = 0

  id = setinsert(bag_box_query, (row + 1, row + interval, iccds[row], iccds[row + interval - 1], archive_id, innerboxs_ids[x], 1, 1))
  bags_ids.append(id)
  
  a+=interval

interval = 1
bag_interval = 10
a = 0
b = 0
c = 0

x = 0
z = 0
v = 0

collection_query = '''
  insert into tblCollectionKit ( ID_Archive_key, ID_Situation_key,ID_Reading_key)
  values (%s,%s,%s);
'''
kit_collection_id = setinsert(collection_query, (archive_id, 1, 1))


kit_box_query = '''
  insert into tblKit 
  (Iccid, ID_BagTen_key, ID_InnerBox_key, ID_OuterBox_key, ID_Archive_key, ID_CollectionKit_key, ID_Situation_key) 
  values (%s,%s,%s,%s,%s,%s,%s)'''

for row in range(0, len(iccds)):
  if (a == bag_interval):
    x +=1
    a = 0
  
  if (b == inner_box_interval):    
    collection_query = '''
      insert into tblCollectionKit ( ID_Archive_key, ID_Situation_key,ID_Reading_key)
      values (%s,%s,%s);
    '''
    kit_collection_id = setinsert(collection_query, (archive_id, 1, 1))

    z += 1
    b = 0 
  
  if (c == 1000):
    v += 1
    c = 0

  a+=1
  b+=1
  c+=1

  setinsert(kit_box_query, (iccds[row], bags_ids[x], innerboxs_ids[z], outerboxes_ids[v], archive_id, kit_collection_id, 1))

end = time.time()
print('{:.2f}'.format(end - start))