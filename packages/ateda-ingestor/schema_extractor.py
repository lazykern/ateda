import json
from avro.datafile import DataFileReader
from avro.io import DatumReader
from pathlib import Path

DIR = Path("/home/lazykern/Downloads/ztf-archive/ztf_public_20240201/")

output = "schema.avsc"
output_wo_cutout = "schema_wo_cutout.avsc"

for file in DIR.glob("*.avro"):
    reader = DataFileReader(open(file, "rb"), DatumReader())
    schema_str = reader.get_meta("avro.schema").decode()
    with open(output, "w") as f:
        f.write(schema_str)

    schema = json.loads(schema_str)
    schema_wo_cutout = schema.copy()
    schema_wo_cutout["fields"] = []
    for field in schema["fields"]:
        if field["name"].startswith("cutout"):
            continue
        schema_wo_cutout["fields"].append(field)
    with open(output_wo_cutout, "w") as f:
        f.write(json.dumps(schema_wo_cutout))
    break
