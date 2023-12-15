import os
import time
import re
import psycopg2

PATH = 'C:\\Users\\Rebeca.UPTECH\\GSM'
ICCIDS = []

def connect_to_db():
    '''
    Conecta ao banco de dados e retorna a conexão
    '''
    return psycopg2.connect(host="192.168.15.13", user="postgres", password="mateme", database="Loader_dev")

def insert_into_database(query, params):
    '''
    Insere um único valor no banco de dados e retorna o ID inserido

    query: Consulta a ser executada
    params: Tupla ou lista com os parâmetros esperados pela consulta
    '''
    database = connect_to_db()
    cursor = database.cursor()

    cursor.execute(query, params)
    database.commit()

    cursor.execute("SELECT lastval();")
    last_id = cursor.fetchone()[0]

    database.close()
    return last_id

def insert_multiple_values(query, params, return_query=''):
    '''
    Insere várias linhas para melhor desempenho e retorna todas as linhas inseridas

    query: Consulta a ser executada
    params: Lista com os parâmetros esperados para todas as linhas
    return_query: Consulta para recuperar os valores inseridos
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
    Lê um arquivo e retorna as informações da ordem e ICCIDs
    
    filename: Nome completo do arquivo a ser lido
    '''
    info_regex = '^(.{1,}): (.{1,})$'
    iccid_regex = '\d{1,}$'

    archive_info = {}

    with open(filename, 'r') as file:
        for line in file.readlines():
            treated_line = line.rstrip('\n').strip()
            info_match = re.match(info_regex, treated_line)

            if info_match:
                if info_match.group(1) in archive_info.keys():
                    archive_info[f'{info_match.group(1)}2'] = info_match.group(2)
                else:
                    archive_info[info_match.group(1)] = info_match.group(2)
            elif re.match(iccid_regex, treated_line):
                ICCIDS.append(treated_line)

    return archive_info

def insert_collection(tableName, archive_id):
    '''
    Insere uma coleção com o nome da tabela fornecido e retorna o ID criado

    tableName: Nome da tabela de coleção a ser inserida
    '''
    query = f'''
        INSERT INTO {tableName} 
        (ID_Archive_key, ID_Situation_key, ID_Reading_key)
        VALUES (%s, %s, %s);
    '''

    return insert_into_database(query, (archive_id, 1, 1))

def insert_values(interval, query, fixed_values, return_query, parent=None):
    '''
    Insere cada Outerbox, InnerBox e Bag criando a relação necessária entre eles

    interval: Quantidade de ICCIDs que cada categoria contém
    query: Consulta para inserir os valores na tabela de categoria
    fixed_values: Parâmetros fixos para inserir na tabela de categoria
    return_query: Consulta para retornar os valores inseridos
    parent: Objeto que contém as informações necessárias do pai
        interval: Quantidade de ICCIDs que o pai contém
        column_name: Nome da coluna de ID do pai
        values: Valores dos IDs do pai
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
    Executa o programa principal que lê o arquivo e insere as informações do arquivo, OuterBoxes, InnerBoxes, Bags, Kits e coleções necessárias
    '''
    for file in os.listdir(PATH):
        if os.path.isfile(os.path.join(PATH, file)) and '.txt' in file:
            global ICCIDS

            start = time.time()
            archive_info = read_file(os.path.join(PATH, file))
            print(f'Exportando o arquivo {file} para o Banco de dados com {len(ICCIDS)} ICCIDs')
            print('Tempo para ler arquivo: {:.2f}s'.format(time.time() - start))

            start = time.time()
            archive_info['ID_Situation_key'] = 1
            query = f'''
                INSERT INTO tblarchive_thales 
                (Item_Code, Item_Description, Customer, Provider, Packaging, CUSTOMER_PO, Batch, QUANTITY, HLR, EAN, Packaging_2, Profile, ID_Situation_key)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            '''

            archive_id = insert_into_database(query, list(archive_info.values()))
            print('Tempo para inserir as informações do arquivo: {:.2f}s'.format(time.time() - start))

            start = time.time()
            collection_id = insert_collection('tblcollectionouterbox_thales', archive_id)

            query = f'''
                INSERT INTO tblouterbox_thales 
                (Starting_quantity, End_Quantity, Initial_iccid, End_iccid, ID_CollectionOuterBox_key, ID_SituationInner_key, ID_Situation_key)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            '''
            return_query = f'''SELECT ID_OuterBox FROM tblouterbox_thales WHERE ID_CollectionOuterBox_Key = {collection_id}'''

            fixed_values = {'ID_CollectionOuterBox_key': collection_id, 'ID_SituationInner_key': 1, 'ID_Situation_key': 1}

            outerboxes = insert_values(interval=1000, query=query, fixed_values=fixed_values, return_query=return_query)
            print('Tempo para inserir Outerboxes: {:.2f}s'.format(time.time() - start))

            start = time.time()
            collection_id = insert_collection('tblcollectioninnerbox_thales', archive_id)

            query = f'''
                INSERT INTO tblinnerbox_thales 
                (Starting_quantity, End_Quantity, Initial_iccid, End_iccid, ID_OuterBox_key, ID_CollectionInnerBox_key, ID_SituationKit_key, ID_Situation_key)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            '''
            return_query = f'''SELECT ID_InnerBox FROM tblinnerbox_thales WHERE ID_CollectionInnerBox_key = {collection_id}'''

            fixed_values = {'ID_CollectionOuterBox_key': collection_id, 'ID_SituationInner_key': 1, 'ID_Situation_key': 1}

            innerboxes = insert_values(
                interval=100,
                query=query,
                fixed_values=fixed_values,
                parent={'values': outerboxes, 'interval': 1000, 'column_name': 'ID_OuterBox_key'},
                return_query=return_query
            )
            print('Tempo para inserir Innerboxes: {:.2f}s'.format(time.time() - start))

            start = time.time()
            query = f'''
                INSERT INTO tblbagten_thales 
                (Starting_quantity, End_Quantity, Initial_iccid, End_iccid, ID_InnerBox_key, ID_Archive_key, ID_SituationKit_key, ID_Situation_key)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            '''
            return_query = f'''SELECT ID_BagTen FROM tblbagten_thales WHERE ID_Archive_key = {archive_id}'''

            fixed_values = {'ID_Archive_key': archive_id, 'ID_SituationInner_key': 1, 'ID_Situation_key': 1}

            bags = insert_values(
                interval=10,
                query=query,
                fixed_values=fixed_values,
                parent={'values': innerboxes, 'interval': 100, 'column_name':'ID_InnerBox_key'},
                return_query=return_query
            )
            print('Tempo para inserir Bags: {:.2f}s'.format(time.time() - start))

            start = time.time()
            query = f'''
                INSERT INTO tblkit_thales 
                (Iccid, ID_Archive_key, ID_CollectionKit_key, ID_Situation_key, ID_BagTen_key, ID_InnerBox_key, ID_OuterBox_key)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            '''
            collection_id = insert_collection('tblcollectionkit_thales', archive_id)

            bag_control, innerbox_control, outerbox_control = (0, 0, 0)
            bag_index, innerbox_index, outerbox_index = (0, 0, 0)
            bag_interval, innerbox_interval, outerbox_interval = (10, 100, 1000)
            values_to_insert = []

            for row in range(0, len(ICCIDS)):
                if bag_control == bag_interval:
                    bag_index += 1
                    bag_control = 0

                if innerbox_control == innerbox_interval:
                    collection_id = insert_collection('tblcollectionkit_thales', archive_id)
                    innerbox_index += 1
                    innerbox_control = 0

                if outerbox_control == outerbox_interval:
                    outerbox_index += 1
                    outerbox_control = 0

                data = {
                    'Iccid': ICCIDS[row],
                    'ID_Archive_key': archive_id,
                    'ID_CollectionKit_key': collection_id,
                    'ID_Situation_key': 1,
                    'ID_BagTen_key': bags[bag_index],
                    'ID_InnerBox_key': innerboxes[innerbox_index],
                    'ID_OuterBox_key': outerboxes[outerbox_index]
                }

                bag_control += 1
                innerbox_control += 1
                outerbox_control += 1

                values_to_insert.append(list(data.values()))

            insert_multiple_values(query, values_to_insert)
            print('Tempo para inserir Kits: {:.2f}s'.format(time.time() - start))

            start = time.time()
            os.rename(os.path.join(PATH, file), f'{PATH}/processados/{file}')

            ICCIDS = []

            print('Tempo para mover o arquivo: {:.2f}s'.format(time.time() - start))
            print(f'Arquivo {file} exportado com sucesso!')
            print('-'*10)

start = time.time()
main()
print('Tempo total: {:.2f}s'.format(time.time() - start))
