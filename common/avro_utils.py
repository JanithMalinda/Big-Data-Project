import io

import fastavro


def load_schema(path):
    return fastavro.schema.load_schema(path)


def serialize(schema, record: dict) -> bytes:
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, record)
    return buf.getvalue()


def deserialize(schema, data: bytes) -> dict:
    buf = io.BytesIO(data)
    return fastavro.schemaless_reader(buf, schema)
