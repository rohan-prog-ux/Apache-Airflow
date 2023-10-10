import Extract as e
import load as l
import json
import transform as t
import dump_to_Database as dtd
print("--extract--")
responses = e.task1_a()
print("--dump_to _database--")
dump_data = dtd.dump_to_Database(responses)
print("--transform--")
result = t.transform(dump_data)
print("--load--")
l.load1_b()
