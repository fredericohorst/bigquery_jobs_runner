
from google.cloud import bigquery
import logging

logging.basicConfig(#level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(funcName)s => %(message)s')

#######################################################################################################################
# LOADING DATA FROM BIGQUERY TO A DATAFRAME USING A SQL FILE

def bq_to_dataframe(sql_file, project_source_id):
    """
    FUNCTION TO LOAD DATA FROM BIGQUERY USING STANDARD SQL AND A SQL FILE
    YOU MUST USE STANDARD SQL
    YOU MUST ENTER THE SQL FILE PATH AND THE PROJECT SOURCE ID AS STRING
    """
    logging.info('bq_to_dataframe started')
    client = bigquery.Client()
    # job execution settings
    job_config = bigquery.QueryJobConfig()
    job_config.allow_large_results = True
    job_config.use_legacy_sql = False
    logging.info('bq_to_dataframe job configuration done')
    # setting the query file
    query = open(sql_file, 'r').read()  
    # job executing and saving to dataframe
    try:
        results = client.query(query, job_config=job_config, project=project_source_id).to_dataframe()
        # logging.info('parameters: jobconfig = {job_config}, project = {project}')
        logging.info('jobconfig: '+str(query_job.job_id))
        logging.info('project: '+str(project))
        if len(results) > 0:
            print('success -- ' + str(len(results)) + ' records found')
        else:
            print('your query returned no results')
        return results
    except Exception as error_value:
        logging.exception('parameters: jobconfig = {job_config}, project = {project}')
        logging.exception('error: ' + str(error_value))
        print('execution error')

#######################################################################################################################
# LOADING DATA TO A BIGQUERY TABLE FROM A BIGQUERY TABLE USING A SQL FILE

def bq_job_runner(sql_file, project, dataset_name, table_name):
    """
    FUNCTION TO CREATE A TABLE IN BIGQUERY FROM A SQL FILE INSTRUCTION
    YOU MUST USE STANDARD SQL
    ALL PARAMETERS MUST BE ENTERED AS STRING
    """
    logging.info('bq_job_runner started')
    client = bigquery.Client()
    # job execution settings
    destination = project + '.' + dataset_name + '.' + table_name
    job_config = bigquery.QueryJobConfig()
    job_config.allow_large_results = True
    job_config.use_legacy_sql = False
    job_config.write_disposition = 'WRITE_TRUNCATE'
    job_config.destination = destination
    logging.info('parameters: job_config='+str(job_config)+', destination='+str(destination))
    logging.info('bq_job_runner job configuration done')
    # setting the query file
    query = open(sql_file, 'r').read()  # (sql_file+LIMIT)
    logging.info('parameters: query='+str(query))
    # job executing
    try:
        query_job = client.query(
            query,
            job_config=job_config,
            project=project,
            job_id_prefix='bq_job_runner_'+ table_name +'_',
        ) 
        # Make an API request.
        logging.info('job sent to bigquery = ' + str(query_job.job_id))
        print("Started job: {}".format(query_job.job_id))
    except Exception as error_value:
        print('ERROR PRINTED: ' + str(error_value))
        logging.error('job not sent to bigquery')
        logging.info('error:' + str(error_value))
        print('job not started')



