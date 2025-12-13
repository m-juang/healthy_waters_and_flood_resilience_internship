"""
debug_thresholds.py

Script untuk memeriksa struktur data threshold yang sebenarnya
dari moata_rain_alerts.py output untuk memahami field apa saja yang tersedia.
"""

import json
from pathlib import Path

def inspect_threshold_structure():
    """Inspect the actual structure of threshold data."""
    
    input_file = Path("moata_output") / "rain_gauges_traces_alarms.json"
    
    if not input_file.exists():
        print(f"ERROR: File not found: {input_file}")
        print("Please run moata_rain_alerts.py first.")
        return
    
    print("Loading data...")
    with open(input_file, 'r') as f:
        all_data = json.load(f)
    
    print(f"Loaded {len(all_data)} gauges\n")
    print("=" * 80)
    print("SEARCHING FOR THRESHOLD DATA STRUCTURES")
    print("=" * 80)
    
    threshold_examples = []
    total_thresholds = 0
    
    for gauge_data in all_data:
        gauge = gauge_data.get("gauge", {})
        gauge_name = gauge.get("name", "Unknown")
        
        traces = gauge_data.get("traces", [])
        
        for trace_data in traces:
            trace = trace_data.get("trace", {})
            trace_name = trace.get("description", "Unknown")
            
            thresholds = trace_data.get("thresholds", [])
            
            if thresholds:
                total_thresholds += len(thresholds)
                
                # Collect first few examples
                if len(threshold_examples) < 5:
                    for threshold in thresholds:
                        threshold_examples.append({
                            "gauge_name": gauge_name,
                            "trace_name": trace_name,
                            "threshold_data": threshold
                        })
                        if len(threshold_examples) >= 5:
                            break
    
    print(f"\nFound {total_thresholds} total thresholds across all gauges")
    print(f"\nShowing first {len(threshold_examples)} threshold examples:\n")
    
    for i, example in enumerate(threshold_examples, 1):
        print("=" * 80)
        print(f"EXAMPLE {i}:")
        print(f"Gauge: {example['gauge_name']}")
        print(f"Trace: {example['trace_name']}")
        print("\nThreshold Data Structure:")
        print(json.dumps(example['threshold_data'], indent=2))
        print("\nAvailable Fields:")
        for key in example['threshold_data'].keys():
            value = example['threshold_data'][key]
            value_type = type(value).__name__
            print(f"  - {key}: {value_type} = {value}")
        print()
    
    # Analyze all unique field names across all thresholds
    print("=" * 80)
    print("ANALYZING ALL THRESHOLD FIELDS")
    print("=" * 80)
    
    all_field_names = set()
    field_value_examples = {}
    
    for gauge_data in all_data:
        traces = gauge_data.get("traces", [])
        for trace_data in traces:
            thresholds = trace_data.get("thresholds", [])
            for threshold in thresholds:
                for field_name in threshold.keys():
                    all_field_names.add(field_name)
                    if field_name not in field_value_examples:
                        field_value_examples[field_name] = threshold[field_name]
    
    print(f"\nAll unique field names found in thresholds ({len(all_field_names)}):")
    for field_name in sorted(all_field_names):
        example_value = field_value_examples.get(field_name, "N/A")
        value_type = type(example_value).__name__
        print(f"  • {field_name:30s} ({value_type:10s}) Example: {example_value}")
    
    # Check for enabled-related fields
    print("\n" + "=" * 80)
    print("CHECKING FOR 'ENABLED' STATUS FIELDS")
    print("=" * 80)
    
    enabled_variants = [
        'enabled', 'isEnabled', 'active', 'isActive', 
        'state', 'status', 'disabled', 'isDisabled'
    ]
    
    found_variants = [v for v in enabled_variants if v in all_field_names]
    
    if found_variants:
        print(f"\n✓ Found {len(found_variants)} enabled-related field(s):")
        for variant in found_variants:
            example_val = field_value_examples.get(variant, "N/A")
            print(f"  • {variant}: {example_val}")
    else:
        print("\n✗ NO enabled-related fields found!")
        print("  This explains why the 'enabled' column is empty in the summary.")
        print("\n  Possible reasons:")
        print("  1. The API endpoint doesn't return enabled status for thresholds")
        print("  2. All thresholds are assumed to be enabled by default")
        print("  3. Enabled status is stored elsewhere (in detailed_alarm data)")
    
    print("\n" + "=" * 80)
    print("RECOMMENDATION")
    print("=" * 80)
    
    if not found_variants:
        print("\nSince thresholds don't have 'enabled' field, you have 2 options:")
        print("  1. Assume all thresholds are enabled (set default to True)")
        print("  2. Cross-reference with detailed_alarm data for enabled status")
        print("  3. Leave it empty (NULL) to indicate 'unknown' status")
    
    print("\n" + "=" * 80)


if __name__ == "__main__":
    inspect_threshold_structure()