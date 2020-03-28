import sys
import os
import logging

logging.basicConfig(#level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(funcName)s => %(message)s')

path = '/bigquery_jobs_runner'
sys.path.append(path+'/config/lib/')
print('main files path: ' + path + '/config/lib/')
logging.info('main files path: ' + path + '/config/lib/')
import bigquery_jobs_runner as etl
print(etl)

if __name__ == '__main__':
    try:
        sql_file = str(os.getenv('sql_file'))
        file_full_path = path + '/' + sql_file
        project = str(os.getenv('GOOGLE_CLOUD_PROJECT'))
        dataset = str(os.getenv('dataset'))
        table = str(os.getenv('table'))
        logging.info('parameters: sql_file='+str(sql_file)+',project='+str(project)+',dataset='+str(dataset)+',table='+str(table)+',file_full_path='+str(file_full_path))
        print(sql_file, project, dataset, table)
        # validando acesso de leitura
        if os.access(file_full_path, os.R_OK) == True:
            print('ok: ' + file_full_path)
            logging.info('file exists: ' + str(file_full_path))
            print('start bq job')
            logging.info('starting library bq job runner')
            etl.bq_job_runner(sql_file = file_full_path, project = project, dataset_name = dataset, table_name = table)
            print('library execution ended')
        else:
            logging.error('file not found: ' + str(file_full_path))
            print('file not found: ' + str(file_full_path))
    except Exception as erro:
        logging.error(erro)
        logging.error('unknown execution error')
        print('unknown execution error')
        pass

