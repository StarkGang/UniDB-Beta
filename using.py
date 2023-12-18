from main import *

# Testing
db = UNIDB(table_name="example_table", sql_lite_url="nicex.db")
db.init()

data_to_insert = {
    "name": "Jane Doe",
    "age": 23,
    "is_student": False,
    "cars": ["bmw"],
    "books": ["mystery"],
}
db.insert(data_to_insert)
result = db.find({"age": 23})
data_to_insert2 = {
    "name": "John Doe",
    "age": 27,
    "is_student": False,
    "cars": ["bmw"],
    "books": ["mystery"],
}
db.insert(data_to_insert2)
data_to_insert3 = {
    "name": "MO e",
    "age": 27,
    "is_student": False,
    "books": ["mystery"],
}
print(result)