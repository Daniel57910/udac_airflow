import configparser
PROJECT_PATH='/Users/danielwork/Documents/GitHub/udac_airflow'

config = configparser.ConfigParser()
config.read(f'{PROJECT_PATH}/dhw.cfg')
conn_string = "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())
print(conn_string)