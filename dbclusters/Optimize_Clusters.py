import requests
import pandas as pd
import os
import json
import copy
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("MyApp").getOrCreate()

class optimize():
    def __init__(self, clusters = 'all', action = 'view', rename = 'yes', idle_time = 10):
        # Get the Databricks workspace host, current user and API token from environment variables
        self.dbk_host = f'https://{spark.conf.get("spark.databricks.workspaceUrl")}'
        self.current_user = spark.sql("SELECT current_user()").collect()[0][0]
        self.token = os.environ.get('DATABRICKS_API_TOKEN')
        if self.token is None:
            raise ValueError("Databricks token is missing. Please set the DATABRICKS_API_TOKEN environmental variable.")
        

        ### Converting input parameters to dictionary so it's easier to handle them
        self.InputParameters = {'idle_time': self.idle_time, 'clusters': self.clusters, 'action': self.action, 'rename': self.rename}
        self.InputParameters = {key: value.lower() if isinstance(value, str) else value for key, value in self.InputParameters.items()}
        
        ### Checking that input parameters are valid
        def check_input_values(self):
            if not (10 <= int(self.InputParameters.get('idle_time')) <= 120):
                raise ValueError("'idle_time' value can't be smaller than 10 or bigger than 120 minutes")
            elif (self.InputParameters.get('clusters') != 'all') & (self.InputParameters.get('clusters') != 'user'):
                raise ValueError("'clusters' can be either 'all' or 'user'")
            elif (self.InputParameters.get('action') != 'update') & (self.InputParameters.get('action') != 'view'):
                raise ValueError("'action' can be either 'update' or 'view'")
            elif (self.InputParameters.get('rename') != 'yes') & (self.InputParameters.get('rename') != 'no'):
                raise ValueError("'rename' can be either 'yes' or 'no'")
            else:
                return True
        
        # Cluster configurations which are being monitored and updated
        self.variable_names = ['num_workers', 'autoscale', 'cluster_name', 'spark_version', 'spark_conf', 'node_type_id', 'driver_node_type_id', 'ssh_public_keys', 'custom_tags', 'cluster_log_conf', 'init_scripts', 'spark_env_vars', 'autotermination_minutes', 'enable_elastic_disk', 'cluster_source', 'instance_pool_id', 'policy_id', 'enable_local_disk_encryption', 'driver_instance_pool_id', 'workload_type', 'runtime_engine', 'docker_image', 'data_security_mode', 'single_user_name', 'cluster_id', 'apply_policy_default_value']

        # Call the GetAllClusters method during initialization to get the list of cluster IDs
        self.ClusterList = self.GetAllClusters()

        # Call the get_cluster_info_for_cluster_ids method to retrieve information for all clusters
        self.Configs = self.get_cluster_info_for_cluster_ids()

        # Automatically remove 'spark.databricks.cluster.profile' from 'spark_conf'
        self.remove_spark_conf_profile()
                
        # Processing Configs
        self.original_Configs, self.ConfigsUpdated = self.process_configs(self.Configs)
        for k,v in self.Configs.items():
            original_config = self.original_Configs.get(k, {})
            self.UpdatedValues = self.get_dict_differences(v, original_config)
            if len(self.UpdatedValues) != 0:
                self.UpdateCluster(k, v)
                print(f"Cluster ID: {k}\nUpdated values: {self.UpdatedValues}\n")

        print("Everything has been optimized")


    def GetAllClusters(self) -> list:
        # This method retrieves the list of cluster IDs available in the Databricks workspace
        api_version = '/api/2.0'
        api_command = '/clusters/list'
        url = f"{self.dbk_host}{api_version}{api_command}"
        headers = {'Authorization': 'Bearer %s' % self.token}
        session = requests.Session()

        payload = {
            "format": "source"
        }

        resp = session.request('GET', url, params=payload, verify=True, headers=headers)
        dfTemp = pd.DataFrame.from_dict(resp.json()['clusters'])
        if self.InputParameters.get('clusters') == 'user':
            dfTemp = dfTemp[dfTemp["creator_user_name"] == self.current_user]
    
        ClusterList = dfTemp['cluster_id'].tolist()

        return ClusterList

    def GetClusterInfo(self, cluster_id: str) -> dict:
        # This method retrieves detailed information about a specific cluster using its ID
        api_version = '/api/2.0'
        api_command = '/clusters/get'
        url = f"{self.dbk_host}{api_version}{api_command}"
        headers = {'Authorization': 'Bearer %s' % self.token}
        session = requests.Session()

        payload = {
            "format": "source",
            "cluster_id": cluster_id,
        }

        resp = session.request('GET', url, params=payload, verify=True, headers=headers)

        # Initialize an empty payload dictionary
        payload = {}

        # Loop through each variable name and set its value using resp_json.get()
        for var_name in self.variable_names:
            payload[var_name] = resp.json().get(var_name, '')

        # Filter out key-value pairs with empty values
        payload = {k: v for k, v in payload.items() if v is not None and v != ''}
        return payload

    def get_cluster_info_for_cluster_ids(self):
        # This method retrieves information for all clusters in the ClusterList
        result = {}
        for cluster_id in self.ClusterList:
            cluster_info = self.GetClusterInfo(cluster_id)
            result[cluster_id] = cluster_info

        # Removes all values from result for which the cluster_source is equal to 'JOB' since it's not possible to update job clusters
        result = {k: v for k, v in result.items() if v.get('cluster_source', '') != 'JOB'}
        return result
    
    def UpdateCluster(self, cluster_id: str, payload: str):
        api_version = '/api/2.0'
        api_command = '/clusters/edit'
        url = f"{self.dbk_host}{api_version}{api_command}"
        headers = {'Authorization': 'Bearer %s' % self.token}
        session = requests.Session()

        resp = session.request('POST', url, data=json.dumps(payload), verify = True, headers=headers) 
        print(cluster_id, resp.json())
    
    def get_dict_differences(self, dict1, dict2):  
        differences = {}
        for key in dict1.keys():
            if key not in dict2:
                differences[key] = (None, dict1[key])
            elif dict1[key] != dict2[key]:
                if isinstance(dict1[key], dict) and isinstance(dict2[key], dict):
                    inner_diff = self.get_dict_differences(dict2[key], dict1[key])
                    if inner_diff:
                        differences[key] = inner_diff
                else:
                    differences[key] = (dict2[key], dict1[key])
        for key in dict2.keys():
            if key not in dict1:
                differences[key] = (dict2[key], None)
        return differences
    
    def remove_spark_conf_profile(self):
        # Remove 'spark.databricks.cluster.profile' from 'spark_conf' for each cluster
        for cluster_id, cluster_info in self.Configs.items():
            spark_conf = cluster_info.get('spark_conf', {})
            spark_conf.pop('spark.databricks.cluster.profile', None)

    def process_configs(self, Configs):
        # Store the original configurations before processing
        self.original_Configs = copy.deepcopy(Configs)

        # Initialize an empty dictionary to store the updated configurations
        self.ConfigsUpdated = {}

        # Process configurations in the self.Configs dictionary
        for k, v in Configs.items():
            if (v.get('num_workers', '') != '') and (v.get('num_workers', '') == 0):
                WorkerTag = "SingleNode"
            elif v.get('autoscale', '') != '':
                WorkerTag = "MultiNode"
                TempVal = v.get('autoscale', '')
                max_workers = TempVal.get('max_workers', '')
                min_workers = TempVal.get('min_workers', '')
                v['autoscale'] = {'min_workers': 1, 'max_workers': max_workers, 'target_workers': 1}
                
            elif v.get('num_workers', '') > 0:
                v['autoscale'] = {'min_workers': 1, 'max_workers': v.get('num_workers', ''), 'target_workers': 1}
                max_workers = v.get('num_workers', '')
                min_workers = 1
                WorkerTag = "MultiNode"
                v.pop('num_workers', None)
            
                
            if 'ml' in v.get('spark_version', ''):
                tag = 'AI'
            else:
                tag = 'Standard'

            v['custom_tags'] = {'Type': tag, 'Workers': WorkerTag}
            v['autotermination_minutes'] = int(self.InputParameters.get('idle_time'))

            if WorkerTag == "SingleNode":
                v['cluster_name'] = v['cluster_name'].split('-')[0] + "-" + tag + "-" + str(v.get('num_workers', '')) + "-dev"
            elif WorkerTag == "MultiNode":
                v['cluster_name'] = v['cluster_name'].split('-')[0] + "-" + tag + "-" + str(min_workers) + "_" + str(max_workers) + "-dev"

            # Store the updated configuration in the ConfigsUpdated dictionary
            self.ConfigsUpdated[k] = v

        return self.original_Configs, self.ConfigsUpdated
