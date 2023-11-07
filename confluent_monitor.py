import requests
import json
import re
from json.decoder import JSONDecodeError


un = "YUCCRCGXTXHHCQ3W"
pw = "KEY_GOES_HERE"

url_in = "https://api.telemetry.confluent.cloud/v2/metrics/cloud/query"
headers = {'Content-type': 'application/json', 'Accept': 'application/json'}

# below metrics from Confluent REST API - https://api.telemetry.confluent.cloud/docs/descriptors/datasets/cloud
metric_names_list_topics = ["io.confluent.kafka.server/received_bytes",
                            "io.confluent.kafka.server/received_records",
                            "io.confluent.kafka.server/retained_bytes",
                            "io.confluent.kafka.server/sent_bytes",
                            "io.confluent.kafka.server/consumer_lag_offsets"]


confluent_cluster_names_list = ["lkc-7pxn7w"]

# below topics will have a different metric path than other topic metrics for separate alerting thresholds
alt_path_topics_list = ["dp.an.clm.cdc.requirement.requirementinfo", "dp.an.nb.cdc.sam.casetracking"]

# all the below JSON payloads are taken from the Confluent REST API
json_payload_schema_registry = ('{ "aggregations": [{ "metric": "io.confluent.kafka.schema_registry/schema_count" }], '
                                '"filter": '
                                '{"field": '
                                '"resource.schema_registry.id", "op": "EQ", "value": "CLUSTER_NAME_PLACEHOLDER"}, '
                                '"granularity": "P11M", "intervals": [ "PT1M/now-2m|m" ], "limit": 25 }')

json_payload_partition_count = ('{ "aggregations": [{ "metric": "io.confluent.kafka.server/partition_count" }], '
                                '"filter": '
                                '{ "field": '
                                '"resource.kafka.id", "op": "EQ", "value": "CLUSTER_NAME_PLACEHOLDER" }, '
                                '"granularity": "PT1M", "intervals": [ "PT1M/now-2m|m" ], "limit": 25 }')

json_payload_cluster_load_percent = ('{ "aggregations":[{"metric": "io.confluent.kafka.server/cluster_load_percent"}], '
                                     '"filter": '
                                     '{ "field": '
                                     '"resource.kafka.id", "op": "EQ", "value": "CLUSTER_NAME_PLACEHOLDER" }, '
                                     '"granularity": "PT1M", "intervals": [ "PT1M/now-2m|m" ], "limit": 25 }')

json_payload_topic = ('{ "aggregations": [ { "metric": "METRIC_NAME_PLACEHOLDER" } ], '
                      '"filter": '
                      '{ "field": "resource.kafka.id", "op": "EQ", "value": "CLUSTER_NAME_PLACEHOLDER" }, '
                      '"granularity": "PT1M", '
                      '"group_by": [ "metric.topic" ], "intervals": [ "PT1M/now-2m|m" ], "limit": 25 }')


def process_non_topic_metric_cluster_load_percent(string_in, metric_name_in, confluent_cluster_list):
    for cluster_name in confluent_cluster_list:
        new_cluster_str = string_in.replace("CLUSTER_NAME_PLACEHOLDER", cluster_name)
        d = json.loads(new_cluster_str)
        r = requests.post(url_in, headers=headers, json=d, auth=(un, pw))
        response = json.loads(r.text)
        if "data" in response:
            key = response["data"]
            for i in key:
                metric_value = i["value"]
                # multiply by 100 and take the int of this, AppD only accepts positive integer values
                metric_value = int(metric_value * 1000)
                print("name=Custom Metrics|Confluent|Cluster|{}|{}, value={}".format(cluster_name, metric_name_in, metric_value))


def process_non_topic_metric(string_in, metric_name_in, confluent_cluster_list):
    for cluster_name in confluent_cluster_list:
        new_cluster_str = string_in.replace("CLUSTER_NAME_PLACEHOLDER", cluster_name)
        d = json.loads(new_cluster_str)
        r = requests.post(url_in, headers=headers, json=d, auth=(un, pw))
        response = json.loads(r.text)
        if "data" in response:
            key = response["data"]
            for i in key:
                metric_value = int(i["value"])
                print("name=Custom Metrics|Confluent|Cluster|{}|{}, value={}".format(cluster_name, metric_name_in, metric_value))


def process_topic_metrics(metrics_list, json_str, confluent_cluster_list, topic_exclusion_list):
    for cluster_name in confluent_cluster_list:
        for i in metrics_list:
            new_cluster_str = json_str.replace("CLUSTER_NAME_PLACEHOLDER", cluster_name)
            new_metric_str = new_cluster_str.replace("METRIC_NAME_PLACEHOLDER", i)
            # regex to get short metric names from line 16 metric_names_list_topics list
            pattern = r'\/([^" \]}]+)'
            match = re.search(pattern, new_metric_str)
            metric_name = "None"
            if match:
                metric_name = match.group(1)
            d = json.loads(new_metric_str)
            r = requests.post(url_in, headers=headers, json=d, auth=(un, pw))
            response = json.loads(r.text)
            if "data" in response:
                key = response["data"]
                for j in key:
                    topic_name = j["metric.topic"]
                    if topic_name not in topic_exclusion_list:
                        metric_value = int(j["value"])
                        print("name=Custom Metrics|Confluent|Cluster|{}|Topic|{}|{}, value={}"
                              .format(cluster_name, topic_name, metric_name, metric_value))
                    elif topic_name in topic_exclusion_list:
                        metric_value = int(j["value"])
                        print("name=Custom Metrics|Confluent|Cluster|{}|alt_topics|Topic|{}|{}, value={}"
                              .format(cluster_name, topic_name, metric_name, metric_value))


process_topic_metrics(metric_names_list_topics, json_payload_topic, confluent_cluster_names_list, alt_path_topics_list)
process_non_topic_metric(json_payload_partition_count, "partition_count", confluent_cluster_names_list)
process_non_topic_metric(json_payload_schema_registry, "schema_registry", confluent_cluster_names_list)
process_non_topic_metric_cluster_load_percent(json_payload_cluster_load_percent,
                                              "cluster_load_1000_percent", confluent_cluster_names_list)
