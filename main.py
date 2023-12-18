import sqlite3
import pymongo
import psycopg2


class UNIDB:
    def __init__(self, table_name, mongo_url=None, sql_url=None, sql_lite_url=None, main_db_name="UniDB"):
        self.main_db_name = main_db_name
        self.table_name = table_name
        self.mongo_url = mongo_url
        self.sql_url = sql_url
        self.sql_lite_url = sql_lite_url

    def init(self):
        if self.mongo_url:
            self.mongo_client = pymongo.MongoClient(self.mongo_url)
            self.mongo_db = self.mongo_client[self.main_db_name]
            self.mongo_table = self.mongo_db[self.table_name]
        if self.sql_url or self.sql_lite_url:
            if self.sql_url:
                self.sql_connection = psycopg2.connect(self.sql_url)
                self.sql_cursor = self.sql_connection.cursor()
                self.sql_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.main_db_name};")
                self.sql_cursor.execute(f"USE {self.main_db_name};")
            if self.sql_lite_url:
                self.sql_lite_connection = sqlite3.connect(self.sql_lite_url)
                self.sql_lite_cursor = self.sql_lite_connection.cursor()

    def python_datatype_to_sql_datatype(self, obj):
        if isinstance(obj, str):
            return "VARCHAR(255)"
        elif isinstance(obj, int):
            return "INTEGER"
        elif isinstance(obj, float):
            return "FLOAT"
        elif isinstance(obj, bool):
            return "BOOLEAN"

    def create_list_table(self, list_name):
        if self.sql_url:
            self.sql_cursor.execute(f"CREATE TABLE IF NOT EXISTS {list_name} (id SERIAL PRIMARY KEY, value VARCHAR(255));")
            self.sql_connection.commit()
        if self.sql_lite_url:
            self.sql_lite_cursor.execute(f"CREATE TABLE IF NOT EXISTS {list_name} (id INTEGER PRIMARY KEY, value TEXT);")
            self.sql_lite_connection.commit()

    def insert_list_values(self, list_name, values):
        if self.sql_url:
            for value in values:
                self.sql_cursor.execute(f"INSERT INTO {list_name} (value) VALUES ('{value}');")
            self.sql_connection.commit()
        if self.sql_lite_url:
            for value in values:
                self.sql_lite_cursor.execute(f"INSERT INTO {list_name} (value) VALUES (?);", (value,))
            self.sql_lite_connection.commit()

    def handle_list(self, key, values):
        hash_value = hash(tuple(values))
        alphanumeric_hash = ''.join(c for c in str(hash_value) if c.isalnum())
        list_table_name = f"{self.table_name}_{key}_{alphanumeric_hash}"
        self.create_list_table(list_table_name)
        self.insert_list_values(list_table_name, values)
        return list_table_name

    def handle_nested_lists(self, data, reverse=False):
        for key, value in data.items():
            if isinstance(value, list):
                if reverse:
                    list_table_name = value
                    self.retrieve_list_values(key, list_table_name, data)
                else:
                    data[key] = self.handle_list(key, value)
        return data

    def create_sub_table(self, sub_table_name):
        if self.sql_url:
            self.sql_cursor.execute(f"CREATE TABLE IF NOT EXISTS {sub_table_name} (id SERIAL PRIMARY KEY, key VARCHAR(255), value TEXT);")
            self.sql_connection.commit()
        if self.sql_lite_url:
            self.sql_lite_cursor.execute(f"CREATE TABLE IF NOT EXISTS {sub_table_name} (id INTEGER PRIMARY KEY, key TEXT, value TEXT);")
            self.sql_lite_connection.commit()

    def insert_sub_data(self, sub_table_name, sub_data):
        if self.sql_url:
            for key, value in sub_data.items():
                self.sql_cursor.execute(f"INSERT INTO {sub_table_name} (key, value) VALUES ('{key}', '{value}');")
            self.sql_connection.commit()
            return self.sql_cursor.lastrowid
        if self.sql_lite_url:
            for key, value in sub_data.items():
                self.sql_lite_cursor.execute(f"INSERT INTO {sub_table_name} (key, value) VALUES (?, ?);", (key, value))
            self.sql_lite_connection.commit()
            return self.sql_lite_cursor.lastrowid

    def retrieve_list_values(self, key, list_table_name, data):
        if self.sql_url:
            self.sql_cursor.execute(f"SELECT value FROM {list_table_name};")
            data[key] = [row[0] for row in self.sql_cursor.fetchall()]
        elif self.sql_lite_url:
            self.sql_lite_cursor.execute(f"SELECT value FROM {list_table_name};")
            data[key] = [row[0] for row in self.sql_lite_cursor.fetchall()]

    def retrieve_sub_data(self, sub_table_name, sub_data_id):
        if self.sql_url:
            self.sql_cursor.execute(f"SELECT * FROM {sub_table_name} WHERE id = {sub_data_id};")
            columns = [desc[0] for desc in self.sql_cursor.description]
            result = dict(zip(columns, self.sql_cursor.fetchone()))
            return result
        elif self.sql_lite_url:
            self.sql_lite_cursor.execute(f"SELECT * FROM {sub_table_name} WHERE id = ?;", (sub_data_id,))
            columns = [desc[0] for desc in self.sql_lite_cursor.description]
            result = dict(zip(columns, self.sql_lite_cursor.fetchone()))
            return result

    def insert(self, data):
        data = self.handle_nested_lists(data)
        if self.mongo_url:
            self.mongo_table.insert_one(data)

        if self.sql_url or self.sql_lite_url:
            if self.sql_url:
                self.sql_cursor.execute(f"SELECT * FROM information_schema.tables WHERE table_name = '{self.table_name}'")
                if not self.sql_cursor.fetchone():
                    create_table_query = f"CREATE TABLE IF NOT EXISTS {self.table_name} (id SERIAL PRIMARY KEY, "
                    create_table_query += ", ".join([f"{key} {self.python_datatype_to_sql_datatype(data[key])}" for key in data.keys()])
                    create_table_query += ");"
                    print(create_table_query)
                    self.sql_cursor.execute(create_table_query)
                    self.sql_connection.commit()
            if self.sql_lite_url:
                self.sql_lite_cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{self.table_name}';")
                if not self.sql_lite_cursor.fetchone():
                    create_table_query = f"CREATE TABLE IF NOT EXISTS {self.table_name} (id INTEGER PRIMARY KEY, "
                    create_table_query += ", ".join([f"{key} {self.python_datatype_to_sql_datatype(data[key])}" for key in data.keys()])
                    create_table_query += ");"
                    print(create_table_query)
                    self.sql_lite_cursor.execute(create_table_query)
                    self.sql_lite_connection.commit()

                value = ""
                key = ""
                for i in data.values():
                    if isinstance(i, str):
                        value += f"'{i}', "
                    else:
                        value += f"{i}, "
                value = value[:-2]
                for i in data.keys():
                    key += f"{i}, "
                key = key[:-2]
                insert_query = f"INSERT INTO {self.table_name} ({key}) VALUES ({', '.join(['?' for _ in data.values()])})"
                print(insert_query)
                values = tuple(data.values())
                if self.sql_url:
                    self.sql_cursor.execute(insert_query, values)
                    self.sql_connection.commit()
            if self.sql_lite_url:
                self.sql_lite_cursor.execute(insert_query, values)
                self.sql_lite_connection.commit()

                for key, value in data.items():
                    if isinstance(value, list):
                        list_table_name = value
                        self.insert_list_values(list_table_name, value)
                    elif isinstance(value, dict):
                        sub_table_name = value
                        sub_data_id = self.insert_sub_data(sub_table_name, value)

    def find(self, condition):
        if self.mongo_url:
            return self.mongo_table.find(condition)
        elif self.sql_url or self.sql_lite_url:
            query = f"SELECT * FROM {self.table_name} WHERE "
            conditions = []
            values = []
            for key, value in condition.items():
                conditions.append(f"{key} = ?")
                values.append(value)
            query += " AND ".join(conditions) + ";"
            print(query)
            if self.sql_url:
                self.sql_cursor.execute(query, values)
                columns = [desc[0] for desc in self.sql_cursor.description]
                results = [dict(zip(columns, row)) for row in self.sql_cursor.fetchall()]
                for result in results:
                    for key, value in result.items():
                        if isinstance(value, str) and value.endswith("_subtable"):
                            sub_table_name = value
                            sub_data_id = int(result[key])
                            sub_data = self.retrieve_sub_data(sub_table_name, sub_data_id)
                            result[key] = sub_data
                        elif isinstance(value, str) and value.startswith(f"{self.table_name}_"):
                            list_table_name = value
                            self.retrieve_list_values(key, list_table_name, result)
                return results
            elif self.sql_lite_url:
                self.sql_lite_cursor.execute(query, values)
                columns = [desc[0] for desc in self.sql_lite_cursor.description]
                results = [dict(zip(columns, row)) for row in self.sql_lite_cursor.fetchall()]
                for result in results:
                    for key, value in result.items():
                        if isinstance(value, str) and value.endswith("_subtable"):
                            sub_table_name = value
                            sub_data_id = int(result[key])
                            sub_data = self.retrieve_sub_data(sub_table_name, sub_data_id)
                            result[key] = sub_data
                        elif isinstance(value, str) and value.startswith(f"{self.table_name}_"):
                            list_table_name = value
                            self.retrieve_list_values(key, list_table_name, result)
                return results
