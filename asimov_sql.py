from tqdm import tqdm
import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, select, Column, text



def update_table(df,tableName,schema,engine):
    print('Creating a list of dicts...')
    dict_list = df.to_dict(orient='records')
    print('List created!')
    
    metadata = MetaData()

    sql_table = Table(
                "CatBoostPred",
                metadata,
                autoload_with=engine,
                extend_existing=True,
                schema=schema
            )
    
    chunk_size = 2000

    def dictList_generator(dictList,chunks):
        for i in range(0, len(dictList), chunks):
            yield dictList[i:i+chunks]
            
    dict_iterator = dictList_generator(dict_list,chunk_size)
    print('Uploading to SQL table '+tableName+'...')

    for rows in tqdm(dict_iterator,total=round(len(dict_list)/chunk_size)):

        with engine.connect() as conn:
            result = conn.execute(sql_table.insert(),rows)
            conn.commit()
            
    print('Completed!')