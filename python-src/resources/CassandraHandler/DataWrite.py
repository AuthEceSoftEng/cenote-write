import json
import datetime
from CassandraHandler.utils.CassandraHandler import CassandraHandler
from CassandraHandler.utils.helpers import get_time_in_ms, time_to_datetime_in_ms


class WriteData:
    """
    This class implements the data write functionality in BDMS.
    """

    def __init__(self):
        """
        Initializes this data writer.
        """
        self.ch = CassandraHandler()
        self.excluded_columns = ["uuid", "items_count", "t_start", "t_stop"]

    def create_table(self, keyspace, table_name, column_specs):
        # Every table should have a uuid column which will be used as primary key
        column_specs.append({"name": "uuid", "type": "UUID", "primary_key": "yes"})

        # Every table should have a timestamp start column, a timestamp end column and an items count column
        column_specs.append({"name": "t_start", "type": "timestamp"})
        column_specs.append({"name": "t_stop", "type": "timestamp"})
        column_specs.append({"name": "items_count", "type": "float"})

        # Every table should have a bdms created_at column, a timestamp end column and an id column
        column_specs.append({"name": "cenote_created_at", "type": "text"})
        column_specs.append({"name": "cenote_timestamp", "type": "text"})
        column_specs.append({"name": "cenote_id", "type": "text"})

        return self.ch.create_table(keyspace, table_name, column_specs)

    def get_column_family(self, url):

        if (url.endswith('/')):
            url = url[:-1]

        info = url.split('/projects/')
        project_id = info[len(info) - 1].split('/events/')[0]
        event_collection = info[len(info) - 1].split('/events/')[1]
        column_family = project_id + '_' + event_collection

        return column_family

    def create_column_specs(self, obj, col_specs=[], prev_key=''):

        for key in obj:
            if (type(obj[key]) is dict):
                if (prev_key != ''):
                    self.create_column_specs(obj[key], col_specs, prev_key + '_' + key)
                else:
                    self.create_column_specs(obj[key], col_specs, key)
            else:
                info = {}
                if (prev_key != ''):
                    info["name"] = prev_key + '_' + key
                else:
                    info["name"] = key
                if (type(obj[key]) is str):
                    info["type"] = "text"
                else:
                    info["type"] = "text"
                col_specs.append(info)

        return col_specs

    def create_data_write_obj(self, obj, data=[], prev_key=''):

        for key in obj:
            if (type(obj[key]) is dict):
                if (prev_key != ''):
                    self.create_data_write_obj(obj[key], data, prev_key + '_' + key)
                else:
                    self.create_data_write_obj(obj[key], data, key)
            else:
                info = {}
                if (prev_key != ''):
                    info["column"] = prev_key + '_' + key
                else:
                    info["column"] = key
                info["value"] = obj[key]

                data.append(info)

        return data

    def append_bdms_info(self, obj, data=[], initial_state=False, curr_state={}):

        if ("timestamp" in obj["cenote"].keys()):
            timestamp = obj["cenote"]["timestamp"]
        else:
            timestamp = get_time_in_ms()

        data.append({"column": "cenote_created_at", "value": obj["cenote"]["created_at"]})
        data.append({"column": "cenote_timestamp", "value": timestamp})
        data.append({"column": "cenote_id", "value": obj["cenote"]["id"]})

        if (initial_state):
            data.append({"column": "uuid", "built_in_function": "now()"})
            data.append({"column": "items_count", "value": 1})
            data.append({"column": "t_start", "value": timestamp})
            data.append({"column": "t_stop", "value": timestamp})
        else:
            data.append({"column": "items_count", "value": curr_state["items_count"] + 1})
            if (type(timestamp) == datetime.datetime):
                if (timestamp < curr_state["t_start"]):
                    data.append({"column": "t_start", "value": timestamp})
                if (timestamp > curr_state["t_stop"]):
                    data.append({"column": "t_stop", "value": timestamp})
            else:
                if (time_to_datetime_in_ms(timestamp) < curr_state["t_start"]):
                    data.append({"column": "t_start", "value": timestamp})
                if (time_to_datetime_in_ms(timestamp) > curr_state["t_stop"]):
                    data.append({"column": "t_stop", "value": timestamp})

        return data

    def write_data(self, keyspace, data_instance):
        data_instance = json.loads(data_instance)
        column_family = self.get_column_family(data_instance["cenote"]["url"])

        if (not self.ch.check_if_table_exists(keyspace, column_family)):
            col_specs = self.create_column_specs(data_instance["data"])
            res = self.create_table(keyspace, column_family, col_specs)

            if (res["response"] == 201):
                data = self.create_data_write_obj(data_instance["data"])
                data = self.append_bdms_info(data_instance, data, initial_state=True)

                for col_data in data:
                    if (col_data["column"] not in self.excluded_columns):
                        col_data["value"] = json.dumps({"values": [col_data["value"]]})

                res = self.ch.write_data(keyspace, column_family, data)

                return res

            else:
                return res
        else:
            query = """
                SELECT * FROM %s.%s where items_count<5000 ALLOW FILTERING
            """ % (keyspace, column_family)

            avail_buckets_info = [item for item in self.ch.execute_query(query)]
            if (len(avail_buckets_info) > 0):

                curr_state = {}

                curr_state["items_count"] = avail_buckets_info[0]["items_count"]
                curr_state["t_start"] = avail_buckets_info[0]["t_start"]
                curr_state["t_stop"] = avail_buckets_info[0]["t_stop"]

                data = self.create_data_write_obj(data_instance["data"], [])
                data = self.append_bdms_info(data_instance, data, initial_state=False, curr_state=curr_state)

                for col_data in data:
                    if (col_data["column"] not in self.excluded_columns):
                        already_existing_data = json.loads(avail_buckets_info[0][col_data["column"]])["values"]
                        already_existing_data.append(col_data["value"])
                        col_data["value"] = json.dumps({"values": already_existing_data})

                res = self.ch.update_data(keyspace, column_family, data, avail_buckets_info[0]["uuid"])

                return res
            else:
                # TODO: Code for cases when all buckets are full
                pass
