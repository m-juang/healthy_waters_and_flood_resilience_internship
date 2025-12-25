import json
from collections import defaultdict
from pathlib import Path

def analyze_structure(data, path="", results=None):
    if results is None:
        results = {
            "paths": defaultdict(lambda: {"count": 0, "types": set(), "sample_values": []}),
            "schemas_by_path": defaultdict(set),
        }
    
    if isinstance(data, list):
        results["paths"][path + "[]"]["count"] += 1
        results["paths"][path + "[]"]["types"].add("list")
        for item in data:
            analyze_structure(item, path + "[]", results)
    
    elif isinstance(data, dict):
        keys = tuple(sorted(data.keys()))
        results["schemas_by_path"][path].add(keys)
        
        for k, v in data.items():
            new_path = f"{path}.{k}" if path else k
            results["paths"][new_path]["count"] += 1
            results["paths"][new_path]["types"].add(type(v).__name__)
            
            # Sample values for primitives
            if isinstance(v, (str, int, float, bool)) and len(results["paths"][new_path]["sample_values"]) < 3:
                results["paths"][new_path]["sample_values"].append(v)
            
            analyze_structure(v, new_path, results)
    
    return results

# Load JSON
with open("outputs/rain_gauges/raw/rain_gauges_traces_alarms.json", "r", encoding="utf-8") as f:
    data = json.load(f)

results = analyze_structure(data)

print("=" * 80)
print("SCHEMA VARIATIONS BY PATH")
print("=" * 80)

for path, schemas in sorted(results["schemas_by_path"].items()):
    if len(schemas) > 1:
        print(f"\n⚠️  HETEROGENEOUS: {path}")
        print(f"   {len(schemas)} different schemas found:")
        for i, schema in enumerate(schemas, 1):
            print(f"   Schema {i}: {schema}")
    elif len(schemas) == 1:
        schema = list(schemas)[0]
        if len(schema) > 0:
            print(f"\n✓ HOMOGENEOUS: {path}")
            print(f"   Keys: {schema}")

print("\n" + "=" * 80)
print("FIELD TYPE VARIATIONS")
print("=" * 80)

for path, info in sorted(results["paths"].items()):
    if len(info["types"]) > 1:
        print(f"\n⚠️  MIXED TYPES: {path}")
        print(f"   Types: {info['types']}")
        print(f"   Count: {info['count']}")
        if info['sample_values']:
            print(f"   Samples: {info['sample_values']}")
