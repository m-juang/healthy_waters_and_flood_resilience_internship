import json

data = json.load(open('moata_output/rain_gauges_traces_alarms.json'))

print("=" * 80)
print("CHECKING FIRST GAUGE'S TRACES")
print("=" * 80)

gauge = data[0]
gauge_name = gauge['gauge'].get('name')
print(f"\nGauge: {gauge_name}")
print(f"Total traces: {len(gauge['traces'])}\n")

for i, trace_data in enumerate(gauge['traces'][:5]):
    trace = trace_data['trace']
    trace_desc = trace.get('description', 'Unknown')
    trace_id = trace.get('id')
    has_alarms_flag = trace.get('hasAlarms', False)
    overflow_alarms = trace_data.get('overflow_alarms', [])
    
    print(f"Trace {i+1}:")
    print(f"  Description: {trace_desc}")
    print(f"  ID: {trace_id}")
    print(f"  hasAlarms flag: {has_alarms_flag}")
    print(f"  Overflow alarms count: {len(overflow_alarms)}")
    
    if overflow_alarms:
        print(f"  First alarm keys: {list(overflow_alarms[0].keys())}")
        print(f"  First alarm: {json.dumps(overflow_alarms[0], indent=4, default=str)}")
    else:
        print(f"  No overflow alarms fetched")
    print()

# Check if any gauge has overflow alarms
print("\n" + "=" * 80)
print("SEARCHING FOR ANY GAUGES WITH OVERFLOW ALARMS")
print("=" * 80)

total_with_overflow = 0
for gauge in data:
    for trace_data in gauge['traces']:
        if trace_data.get('overflow_alarms'):
            total_with_overflow += 1
            print(f"\nGauge: {gauge['gauge'].get('name')}")
            print(f"Trace: {trace_data['trace'].get('description')}")
            print(f"Overflow alarms: {len(trace_data['overflow_alarms'])}")
            if trace_data['overflow_alarms']:
                print(f"First alarm: {json.dumps(trace_data['overflow_alarms'][0], indent=2, default=str)}")
            break
    if total_with_overflow >= 3:
        break

print(f"\nTotal traces with overflow alarms found: {total_with_overflow}")
