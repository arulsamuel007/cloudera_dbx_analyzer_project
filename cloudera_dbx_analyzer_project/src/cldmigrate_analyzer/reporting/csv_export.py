"""
CSV Export Module - Complete Analysis Results

Exports all analysis results to CSV files for easy consumption in Excel,
data analysis tools, or other systems.

Generates multiple CSV files:
1. files_inventory.csv - Complete file listing with all metadata
2. database_tables.csv - All table references with context
3. sql_complexity.csv - SQL query complexity metrics
4. variables.csv - All variables found
5. connections.csv - JDBC, URLs, Kafka, Storage paths
6. master_summary.csv - High-level summary metrics
"""

import csv
import json
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime


def format_size(bytes_val: int) -> str:
    """Format bytes into human-readable size"""
    if bytes_val is None:
        return ""
    if bytes_val < 1024:
        return f"{bytes_val} B"
    elif bytes_val < 1024 * 1024:
        return f"{bytes_val / 1024:.1f} KB"
    else:
        return f"{bytes_val / (1024 * 1024):.2f} MB"


def export_files_inventory(files_index: List[Dict], output_path: Path) -> int:
    """
    Export complete file inventory to CSV.
    
    Columns:
    - File Path
    - File Name
    - Directory
    - Type
    - Size (Bytes)
    - Size (Formatted)
    - Lines
    - Words
    - Parse Status
    - Has Streaming
    - Has Dynamic SQL
    """
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # Header
        writer.writerow([
            'File Path',
            'File Name',
            'Directory',
            'Type',
            'Size (Bytes)',
            'Size (Formatted)',
            'Lines',
            'Words',
            'Parse Status',
            'Has Streaming',
            'Has Dynamic SQL'
        ])
        
        # Data
        for file_info in files_index:
            path = file_info.get('path', '')
            path_obj = Path(path)
            
            writer.writerow([
                path,
                path_obj.name if path else '',
                str(path_obj.parent) if path else '',
                file_info.get('detected_type', ''),
                file_info.get('size_bytes', ''),
                format_size(file_info.get('size_bytes', 0)),
                file_info.get('lines_count', ''),
                file_info.get('words_count', ''),
                file_info.get('parse_status', ''),
                'Yes' if file_info.get('has_streaming') else 'No',
                'Yes' if file_info.get('has_dynamic_sql') else 'No'
            ])
    
    return len(files_index)


def export_database_tables(database_context: Dict, output_path: Path) -> int:
    """
    Export all table references (source and target) to CSV.
    
    Columns:
    - Table Type (Source/Target)
    - Full Name
    - Database
    - Schema
    - Table Name
    - Operation
    - File
    - Line Number
    - Confidence
    - Has Variables
    """
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # Header
        writer.writerow([
            'Table Type',
            'Full Name',
            'Database',
            'Schema',
            'Table Name',
            'Operation',
            'File',
            'Line Number',
            'Confidence',
            'Has Variables'
        ])
        
        count = 0
        
        # Source tables
        for table in database_context.get('source_tables', []):
            writer.writerow([
                'Source (Read)',
                table.get('full_name', ''),
                table.get('database', ''),
                table.get('schema', ''),
                table.get('table', ''),
                table.get('operation', ''),
                table.get('file', ''),
                table.get('line_number', ''),
                table.get('confidence', ''),
                'Yes' if table.get('has_variables') else 'No'
            ])
            count += 1
        
        # Target tables
        for table in database_context.get('target_tables', []):
            writer.writerow([
                'Target (Write)',
                table.get('full_name', ''),
                table.get('database', ''),
                table.get('schema', ''),
                table.get('table', ''),
                table.get('operation', ''),
                table.get('file', ''),
                table.get('line_number', ''),
                table.get('confidence', ''),
                'Yes' if table.get('has_variables') else 'No'
            ])
            count += 1
    
    return count


def export_sql_complexity(sql_complexity: Dict, output_path: Path) -> int:
    """
    Export SQL complexity analysis to CSV.
    
    Columns:
    - File
    - Complexity Level
    - Total Score
    - Query Lines
    - Query Length
    - Total JOINs
    - JOIN Types
    - Total Subqueries
    - Max Subquery Depth
    - Correlated Subqueries
    - Total CTEs
    - Recursive CTEs
    - Window Functions
    - Window Function Types
    - Total Aggregates
    - Has GROUP BY
    - Has HAVING
    - Set Operations
    - CASE Statements
    - Has DDL
    - Execution Complexity
    - Risk Flags
    """
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # Header
        writer.writerow([
            'File',
            'Complexity Level',
            'Total Score',
            'Query Lines',
            'Query Length (chars)',
            'Total JOINs',
            'JOIN Types',
            'Total Subqueries',
            'Max Subquery Depth',
            'Correlated Subqueries',
            'Total CTEs',
            'Recursive CTEs',
            'Window Functions',
            'Window Function Types',
            'Total Aggregates',
            'Has GROUP BY',
            'Has HAVING',
            'Set Operations',
            'CASE Statements',
            'Has DDL',
            'Execution Complexity',
            'Risk Flags'
        ])
        
        count = 0
        
        # Data
        for query in sql_complexity.get('detailed_results', []):
            join_types = ', '.join(f"{k}:{v}" for k, v in query.get('join_analysis', {}).get('join_types', {}).items())
            window_types = ', '.join(query.get('window_function_analysis', {}).get('window_function_types', []))
            risk_flags = ', '.join(query.get('risk_flags', []))
            
            writer.writerow([
                query.get('file_path', ''),
                query.get('complexity_level', ''),
                query.get('total_complexity_score', ''),
                query.get('query_lines', ''),
                query.get('query_length', ''),
                query.get('join_analysis', {}).get('total_joins', 0),
                join_types,
                query.get('subquery_analysis', {}).get('total_subqueries', 0),
                query.get('subquery_analysis', {}).get('max_nesting_depth', 0),
                query.get('subquery_analysis', {}).get('correlated_subqueries', 0),
                query.get('cte_analysis', {}).get('total_ctes', 0),
                query.get('cte_analysis', {}).get('recursive_ctes', 0),
                query.get('window_function_analysis', {}).get('total_window_functions', 0),
                window_types,
                query.get('aggregate_analysis', {}).get('total_aggregates', 0),
                'Yes' if query.get('aggregate_analysis', {}).get('has_group_by') else 'No',
                'Yes' if query.get('aggregate_analysis', {}).get('has_having') else 'No',
                query.get('set_operation_analysis', {}).get('total_set_operations', 0),
                query.get('control_structure_analysis', {}).get('total_case_statements', 0),
                'Yes' if query.get('ddl_analysis', {}).get('has_create') else 'No',
                query.get('estimated_execution_complexity', ''),
                risk_flags
            ])
            count += 1
    
    return count


def export_variables(database_context: Dict, output_path: Path) -> int:
    """
    Export all variables found to CSV.
    
    Columns:
    - Variable Name
    - Variable Type (hiveconf, hivevar, simple)
    - Usage Count (approximate from tables)
    """
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # Header
        writer.writerow([
            'Variable Name',
            'Variable Type',
            'Full Syntax'
        ])
        
        count = 0
        
        # Get variables
        variables = database_context.get('variables_found', [])
        
        for var in variables:
            # Determine type
            if ':' in var:
                var_type = var.split(':')[0]
                var_name = var.split(':', 1)[1]
                full_syntax = f"${{{var}}}"
            else:
                var_type = 'simple'
                var_name = var
                full_syntax = f"${{{var}}}"
            
            writer.writerow([
                var_name,
                var_type,
                full_syntax
            ])
            count += 1
    
    return count


def export_connections(findings: Dict, output_path: Path) -> int:
    """
    Export all connections (JDBC, URLs, Kafka, Storage) to CSV.
    
    Columns:
    - Connection Type
    - Value
    - File
    - Line
    - Confidence
    """
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # Header
        writer.writerow([
            'Connection Type',
            'Value',
            'File',
            'Line',
            'Confidence'
        ])
        
        count = 0
        
        # JDBC
        for item in findings.get('jdbc_strings', []):
            writer.writerow([
                'JDBC Connection',
                item.get('value', ''),
                item.get('file', ''),
                item.get('line', ''),
                item.get('confidence', '')
            ])
            count += 1
        
        # URLs
        for item in findings.get('urls', []):
            writer.writerow([
                'URL',
                item.get('value', ''),
                item.get('file', ''),
                item.get('line', ''),
                item.get('confidence', '')
            ])
            count += 1
        
        # Kafka
        for item in findings.get('kafka_bootstrap_hints', []):
            writer.writerow([
                'Kafka Bootstrap Server',
                item.get('value', ''),
                item.get('file', ''),
                item.get('line', ''),
                item.get('confidence', '')
            ])
            count += 1
        
        # Storage Paths
        for item in findings.get('storage_paths', []):
            writer.writerow([
                'Storage Path',
                item.get('value', ''),
                item.get('file', ''),
                item.get('line', ''),
                item.get('confidence', '')
            ])
            count += 1
    
    return count


def export_master_summary(
    repo_summary: Dict,
    database_context: Dict,
    sql_complexity: Dict,
    complexity: Dict,
    output_path: Path
) -> int:
    """
    Export high-level summary metrics to CSV.
    
    Two-column format: Metric Name, Value
    """
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # Header
        writer.writerow(['Metric', 'Value'])
        
        # Repository Info
        writer.writerow(['Repository Path', repo_summary.get('repo_root', '')])
        writer.writerow(['Generated At', datetime.fromtimestamp(repo_summary.get('generated_at_epoch', 0)).strftime('%Y-%m-%d %H:%M:%S')])
        writer.writerow(['Analysis Duration (seconds)', repo_summary.get('elapsed_seconds', 0)])
        writer.writerow(['', ''])  # Blank line
        
        # File Statistics
        writer.writerow(['Total Files', repo_summary.get('file_count', 0)])
        writer.writerow(['Oozie Workflows', repo_summary.get('workflow_count', 0)])
        writer.writerow(['Coordinators', repo_summary.get('coordinator_count', 0)])
        writer.writerow(['Bundles', repo_summary.get('bundle_count', 0)])
        writer.writerow(['Has Streaming', 'Yes' if repo_summary.get('has_streaming') else 'No'])
        writer.writerow(['Has Dynamic SQL', 'Yes' if repo_summary.get('has_dynamic_sql') else 'No'])
        writer.writerow(['', ''])
        
        # Database Statistics
        db_summary = database_context.get('summary', {})
        writer.writerow(['Total Databases', db_summary.get('total_databases', 0)])
        writer.writerow(['Total Schemas', db_summary.get('total_schemas', 0)])
        writer.writerow(['Source Table References', db_summary.get('total_source_table_refs', 0)])
        writer.writerow(['Target Table References', db_summary.get('total_target_table_refs', 0)])
        writer.writerow(['Variables Found', db_summary.get('total_variables', 0)])
        writer.writerow(['', ''])
        
        # SQL Complexity Statistics
        if sql_complexity:
            writer.writerow(['SQL Queries Analyzed', sql_complexity.get('queries_analyzed', 0)])
            writer.writerow(['Average SQL Complexity', f"{sql_complexity.get('average_complexity_score', 0):.1f}"])
            
            dist = sql_complexity.get('complexity_distribution', {})
            writer.writerow(['Simple Queries', dist.get('simple', 0)])
            writer.writerow(['Moderate Queries', dist.get('moderate', 0)])
            writer.writerow(['Complex Queries', dist.get('complex', 0)])
            writer.writerow(['Very Complex Queries', dist.get('very_complex', 0)])
            
            metrics = sql_complexity.get('aggregated_metrics', {})
            writer.writerow(['Total JOINs', metrics.get('total_joins', 0)])
            writer.writerow(['Total Subqueries', metrics.get('total_subqueries', 0)])
            writer.writerow(['Total CTEs', metrics.get('total_ctes', 0)])
            writer.writerow(['Total Window Functions', metrics.get('total_window_functions', 0)])
            writer.writerow(['', ''])
        
        # Overall Complexity
        if complexity:
            writer.writerow(['Repository Complexity Level', complexity.get('repo_level', '').upper()])
            writer.writerow(['Repository Complexity Score', complexity.get('repo_score', 0)])
    
    return 1  # One summary file


def export_gaps(
    files_index: List[Dict],
    unresolved_vars: List[Dict],
    partial_vars: List[Dict],
    sql_complexity: Dict,
    complexity: Dict,
    variables_data: Dict = None,  # Optional: variables.json data with by_file mapping
    database_context: Dict = None,  # Optional: database context for table-level gaps
    findings: Dict = None,  # Optional: findings for connection/pattern gaps
    output_path: Path = None
) -> int:
    """
    Export identified gaps per file to CSV.
    
    Columns:
    - File Name
    - File Path
    - Gap Type
    - Gap Description
    - Gap Details
    - Line Number (if applicable)
    - Severity
    """
    if output_path is None:
        raise ValueError("output_path is required")
    
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # Header
        writer.writerow([
            'File Name',
            'File Path',
            'Gap Type',
            'Gap Description',
            'Gap Details',
            'Line Number',
            'Severity'
        ])
        
        count = 0
        
        # Debug: Log what we're working with (for troubleshooting)
        # Note: This won't show in production but helps identify issues
        files_count = len(files_index) if isinstance(files_index, list) else 0
        unresolved_count = len(unresolved_vars) if isinstance(unresolved_vars, list) else 0
        partial_count = len(partial_vars) if isinstance(partial_vars, list) else 0
        
        # Ensure files_index is a list
        if not isinstance(files_index, list):
            files_index = []
        
        # Create a file index for quick lookup (normalize paths)
        file_lookup = {}
        for file_info in files_index:
            if not isinstance(file_info, dict):
                continue
            path = file_info.get('path', '')
            if path:
                # Normalize path separators
                normalized_path = path.replace('\\', '/')
                file_lookup[normalized_path] = file_info
                file_lookup[path] = file_info  # Also keep original
        
        # Helper to normalize paths
        def normalize_path(p):
            if not p:
                return ''
            return p.replace('\\', '/')
        
        # Ensure unresolved_vars and partial_vars are lists
        if not isinstance(unresolved_vars, list):
            unresolved_vars = []
        if not isinstance(partial_vars, list):
            partial_vars = []
        
        # 1. Unresolved variables per file
        file_unresolved = {}
        
        # First, try to get file associations from variables.json if available
        if variables_data and isinstance(variables_data, dict) and 'by_file' in variables_data:
            for file_path, vars_list in variables_data['by_file'].items():
                norm_file_path = normalize_path(file_path)
                if not vars_list:
                    continue
                for var_name in vars_list:
                    if not var_name:
                        continue
                    # Check if this variable is unresolved
                    for var in unresolved_vars:
                        var_name_from_unresolved = var.get('name', '')
                        # Handle different variable name formats
                        if (var_name == var_name_from_unresolved or 
                            var_name in var_name_from_unresolved or 
                            var_name_from_unresolved in var_name):
                            if norm_file_path not in file_unresolved:
                                file_unresolved[norm_file_path] = []
                            # Avoid duplicates
                            if not any(v['name'] == var_name for v in file_unresolved[norm_file_path]):
                                file_unresolved[norm_file_path].append({
                                    'name': var_name,
                                    'reason': var.get('reason', 'Cannot be resolved')
                                })
                            break
        
        # Also check definitions_found to get source files where variables are defined
        for var in unresolved_vars:
            var_name = var.get('name', '')
            if not var_name:
                continue
            definitions = var.get('definitions_found', [])
            if definitions:
                # Variable has definitions but still unresolved (circular dependency, etc.)
                for defn in definitions:
                    source_file = defn.get('defined_in', '')
                    if source_file:
                        norm_source_file = normalize_path(source_file)
                        if norm_source_file not in file_unresolved:
                            file_unresolved[norm_source_file] = []
                        # Avoid duplicates
                        if not any(v['name'] == var_name for v in file_unresolved[norm_source_file]):
                            file_unresolved[norm_source_file].append({
                                'name': var_name,
                                'reason': var.get('reason', 'Cannot be resolved')
                            })
            else:
                # Variable has no definitions - completely missing
                # Try to find usage from variables.json
                if variables_data and 'by_file' in variables_data:
                    for file_path, vars_list in variables_data['by_file'].items():
                        if vars_list and var_name in vars_list:
                            norm_file_path = normalize_path(file_path)
                            if norm_file_path not in file_unresolved:
                                file_unresolved[norm_file_path] = []
                            if not any(v['name'] == var_name for v in file_unresolved[norm_file_path]):
                                file_unresolved[norm_file_path].append({
                                    'name': var_name,
                                    'reason': 'Variable used but never defined'
                                })
        
        # Fallback: if we have unresolved vars but no file associations, create entries anyway
        if not file_unresolved and unresolved_vars:
            # Create a generic entry for unresolved variables without file context
            for var in unresolved_vars:
                var_name = var.get('name', '')
                if var_name:
                    # Use a placeholder file path
                    placeholder_path = 'unknown'
                    if placeholder_path not in file_unresolved:
                        file_unresolved[placeholder_path] = []
                    file_unresolved[placeholder_path].append({
                        'name': var_name,
                        'reason': var.get('reason', 'Cannot be resolved - no file context available')
                    })
        
        # Also check unresolved_hits from findings
        # (This would need to be passed separately, but we'll work with what we have)
        
        # Write unresolved variables
        for file_path, vars_list in file_unresolved.items():
            if not vars_list:
                continue
            # Try to get file info, handle both normalized and original paths
            file_info = file_lookup.get(file_path) or file_lookup.get(normalize_path(file_path), {})
            file_name = Path(file_path).name if file_path and file_path != 'unknown' else (file_info.get('path', '') or 'unknown')
            if file_name == 'unknown' and file_path != 'unknown':
                file_name = Path(file_path).name
            
            for var_info in vars_list:
                writer.writerow([
                    file_name,
                    file_path if file_path != 'unknown' else 'Variable found in repository but file context unavailable',
                    'Unresolved Variable',
                    f"Variable '{var_info['name']}' cannot be resolved",
                    f"Reason: {var_info['reason']}",
                    '',
                    'High'
                ])
                count += 1
        
        # 2. Partially resolved variables
        file_partial = {}
        for var in partial_vars:
            var_name = var.get('name', '')
            if not var_name:
                continue
            definitions = var.get('definitions', [])
            if definitions:
                for defn in definitions:
                    source_file = defn.get('defined_in', '')
                    if source_file:
                        norm_source_file = normalize_path(source_file)
                        if norm_source_file not in file_partial:
                            file_partial[norm_source_file] = []
                        # Avoid duplicates
                        if not any(v['name'] == var_name for v in file_partial[norm_source_file]):
                            file_partial[norm_source_file].append({
                                'name': var_name,
                                'unresolved_parts': var.get('unresolved_parts', [])
                            })
            else:
                # No definitions but marked as partial - add to unknown
                if 'unknown' not in file_partial:
                    file_partial['unknown'] = []
                file_partial['unknown'].append({
                    'name': var_name,
                    'unresolved_parts': var.get('unresolved_parts', [])
                })
        
        for file_path, vars_list in file_partial.items():
            if not vars_list:
                continue
            file_info = file_lookup.get(file_path) or file_lookup.get(normalize_path(file_path), {})
            file_name = Path(file_path).name if file_path and file_path != 'unknown' else (file_info.get('path', '') or 'unknown')
            if file_name == 'unknown' and file_path != 'unknown':
                file_name = Path(file_path).name
            
            for var_info in vars_list:
                unresolved_parts = var_info.get('unresolved_parts', [])
                unresolved_str = ', '.join(str(p) for p in unresolved_parts) if unresolved_parts else 'Unknown parts'
                writer.writerow([
                    file_name,
                    file_path if file_path != 'unknown' else 'Variable found in repository but file context unavailable',
                    'Partially Resolved Variable',
                    f"Variable '{var_info['name']}' has unresolved parts",
                    f"Unresolved parts: {unresolved_str}",
                    '',
                    'Medium'
                ])
                count += 1
        
        # 3. Parse errors
        for file_info in files_index:
            path = file_info.get('path', '')
            parse_status = file_info.get('parse_status', 'ok')
            parse_message = file_info.get('parse_message', '')
            
            # Check for parse errors - be more lenient with status checks
            if parse_status and parse_status.lower() not in ('ok', 'pending', 'success'):
                file_name = Path(path).name if path else 'unknown'
                writer.writerow([
                    file_name,
                    path or 'unknown',
                    'Parse Error',
                    f"File parsing failed: {parse_status}",
                    parse_message or 'No details available',
                    '',
                    'High'
                ])
                count += 1
        
        # 4. High complexity SQL queries
        if sql_complexity and isinstance(sql_complexity, dict):
            detailed_results = sql_complexity.get('detailed_results', [])
            if not isinstance(detailed_results, list):
                detailed_results = []
            for query in detailed_results:
                if not isinstance(query, dict):
                    continue
                file_path = query.get('file_path', '')
                complexity_level = query.get('complexity_level', '')
                total_score = query.get('total_complexity_score', 0)
                
                # Check for high complexity
                if complexity_level and complexity_level.lower() in ('complex', 'very_complex'):
                    file_name = Path(file_path).name if file_path else 'unknown'
                    risk_flags = query.get('risk_flags', [])
                    risk_flags_str = ', '.join(str(f) for f in risk_flags) if risk_flags else 'No specific risk flags'
                    
                    writer.writerow([
                        file_name,
                        file_path or 'unknown',
                        'High SQL Complexity',
                        f"SQL query has {complexity_level} complexity (score: {total_score})",
                        f"Risk flags: {risk_flags_str}",
                        str(query.get('line_start', '')) if query.get('line_start') else '',
                        'High' if complexity_level.lower() == 'very_complex' else 'Medium'
                    ])
                    count += 1
        
        # 5. High complexity files (from complexity.json)
        if complexity and isinstance(complexity, dict):
            items = complexity.get('items', [])
            if not isinstance(items, list):
                items = []
            for item in items:
                if not isinstance(item, dict):
                    continue
                item_id = item.get('item_id', '')
                level = item.get('level', '')
                total_score = item.get('total_score', 0)
                risk_flags = item.get('risk_flags', [])
                
                # Check if it's a file (not a workflow)
                if item_id and item_id.startswith('file:'):
                    file_path = item_id.replace('file:', '')
                    norm_file_path = normalize_path(file_path)
                    file_info = file_lookup.get(norm_file_path) or file_lookup.get(file_path, {})
                    file_name = Path(file_path).name if file_path else 'unknown'
                    
                    if level and level.lower() in ('complex', 'very_complex'):
                        risk_flags_str = ', '.join(str(f) for f in risk_flags) if risk_flags else 'None'
                        writer.writerow([
                            file_name,
                            file_path or 'unknown',
                            'High File Complexity',
                            f"File has {level} complexity (score: {total_score})",
                            f"Risk flags: {risk_flags_str}",
                            '',
                            'High' if level.lower() == 'very_complex' else 'Medium'
                        ])
                        count += 1
        
        # 6. Files with streaming (migration consideration)
        for file_info in files_index:
            if file_info.get('has_streaming'):
                path = file_info.get('path', '')
                file_name = Path(path).name if path else 'unknown'
                writer.writerow([
                    file_name,
                    path or 'unknown',
                    'Streaming Code Detected',
                    'File contains streaming code (readStream/writeStream)',
                    'Requires special attention for Databricks migration (Streaming APIs)',
                    '',
                    'Medium'
                ])
                count += 1
        
        # 7. Files with dynamic SQL
        for file_info in files_index:
            if file_info.get('has_dynamic_sql'):
                path = file_info.get('path', '')
                file_name = Path(path).name if path else 'unknown'
                writer.writerow([
                    file_name,
                    path or 'unknown',
                    'Dynamic SQL Detected',
                    'File contains dynamically constructed SQL',
                    'May require refactoring for Databricks compatibility',
                    '',
                    'Medium'
                ])
                count += 1
        
        # 8. Database tables with variables (migration consideration)
        if database_context and isinstance(database_context, dict):
            # Source tables with variables
            for table in database_context.get('source_tables', []):
                if table.get('has_variables'):
                    file_path = table.get('file', '')
                    file_name = Path(file_path).name if file_path else 'unknown'
                    table_name = table.get('full_name', table.get('table', 'unknown'))
                    writer.writerow([
                        file_name,
                        file_path or 'unknown',
                        'Table with Variables',
                        f"Source table '{table_name}' contains variables",
                        f"Variables in table reference may need resolution for Databricks",
                        str(table.get('line_number', '')) if table.get('line_number') else '',
                        'Medium'
                    ])
                    count += 1
            
            # Target tables with variables
            for table in database_context.get('target_tables', []):
                if table.get('has_variables'):
                    file_path = table.get('file', '')
                    file_name = Path(file_path).name if file_path else 'unknown'
                    table_name = table.get('full_name', table.get('table', 'unknown'))
                    writer.writerow([
                        file_name,
                        file_path or 'unknown',
                        'Table with Variables',
                        f"Target table '{table_name}' contains variables",
                        f"Variables in table reference may need resolution for Databricks",
                        str(table.get('line_number', '')) if table.get('line_number') else '',
                        'Medium'
                    ])
                    count += 1
        
        # 9. JDBC connections (migration consideration - need to update for Databricks)
        if findings and isinstance(findings, dict):
            for jdbc in findings.get('jdbc_strings', []):
                file_path = jdbc.get('file', '')
                file_name = Path(file_path).name if file_path else 'unknown'
                jdbc_value = jdbc.get('value', '')
                writer.writerow([
                    file_name,
                    file_path or 'unknown',
                    'JDBC Connection',
                    'JDBC connection string detected',
                    f"Connection: {jdbc_value[:100]}..." if len(jdbc_value) > 100 else f"Connection: {jdbc_value}",
                    str(jdbc.get('line', '')) if jdbc.get('line') else '',
                    'High'
                ])
                count += 1
            
            # Kafka bootstrap servers
            for kafka in findings.get('kafka_bootstrap_hints', []):
                file_path = kafka.get('file', '')
                file_name = Path(file_path).name if file_path else 'unknown'
                kafka_value = kafka.get('value', '')
                writer.writerow([
                    file_name,
                    file_path or 'unknown',
                    'Kafka Connection',
                    'Kafka bootstrap server detected',
                    f"Server: {kafka_value}",
                    str(kafka.get('line', '')) if kafka.get('line') else '',
                    'Medium'
                ])
                count += 1

            # URLs
            for url in findings.get('urls', []):
                file_path = url.get('file', '')
                file_name = Path(file_path).name if file_path else 'unknown'
                url_value = url.get('value', '')
                writer.writerow([
                    file_name,
                    file_path or 'unknown',
                    'URL Detected',
                    'URL detected in code or config',
                    f"URL: {url_value[:100]}..." if len(url_value) > 100 else f"URL: {url_value}",
                    str(url.get('line', '')) if url.get('line') else '',
                    'Medium'
                ])
                count += 1

            # Storage paths
            for storage in findings.get('storage_paths', []):
                file_path = storage.get('file', '')
                file_name = Path(file_path).name if file_path else 'unknown'
                storage_value = storage.get('value', '')
                writer.writerow([
                    file_name,
                    file_path or 'unknown',
                    'Storage Path Detected',
                    'Storage or filesystem path detected',
                    f"Path: {storage_value}",
                    str(storage.get('line', '')) if storage.get('line') else '',
                    'Medium'
                ])
                count += 1
        
        # 10. All unresolved variables (even without file context)
        # This ensures we capture all unresolved vars shown in HTML
        for var in unresolved_vars:
            var_name = var.get('name', '')
            if not var_name:
                continue
            
            # Check if we already added this variable
            already_added = False
            for file_path, vars_list in file_unresolved.items():
                if any(v['name'] == var_name for v in vars_list):
                    already_added = True
                    break
            
            if not already_added:
                # Try to find file from definitions
                definitions = var.get('definitions_found', [])
                if definitions:
                    source_file = definitions[0].get('defined_in', '')
                    file_name = Path(source_file).name if source_file else 'unknown'
                    writer.writerow([
                        file_name,
                        source_file or 'unknown',
                        'Unresolved Variable',
                        f"Variable '{var_name}' cannot be resolved",
                        f"Reason: {var.get('reason', 'Cannot be resolved')}",
                        '',
                        'High'
                    ])
                    count += 1
                elif variables_data and 'by_file' in variables_data:
                    # Find file from variables.json
                    for file_path, vars_list in variables_data['by_file'].items():
                        if var_name in vars_list:
                            file_name = Path(file_path).name if file_path else 'unknown'
                            writer.writerow([
                                file_name,
                                file_path or 'unknown',
                                'Unresolved Variable',
                                f"Variable '{var_name}' cannot be resolved",
                                'Variable used but never defined',
                                '',
                                'High'
                            ])
                            count += 1
                            break
        
        # If no gaps found, try to add at least some basic information
        if count == 0:
            # Check if we have any files at all
            if files_index:
                # Add a summary row indicating analysis completed but no gaps found
                writer.writerow([
                    'Summary',
                    f'{len(files_index)} files analyzed',
                    'No Gaps Found',
                    'No migration gaps identified in this analysis',
                    f'Analyzed {len(files_index)} files. All passed basic checks.',
                    '',
                    'Info'
                ])
                count = 1
            else:
                # No files at all - this is unusual
                writer.writerow([
                    'N/A',
                    'N/A',
                    'No Files Analyzed',
                    'No files were found in the repository',
                    'Please check the input directory and file filters',
                    '',
                    'Warning'
                ])
                count = 1
    
    return count


def export_all_to_csv(artifacts_dir: Path, output_dir: Path) -> Dict[str, int]:
    """
    Export all analysis results to CSV files.
    
    Args:
        artifacts_dir: Path to artifacts directory
        output_dir: Path where CSV files will be saved
        
    Returns:
        Dictionary with counts of exported items per file
    """
    
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    results = {}
    
    # Load artifacts
    try:
        with open(artifacts_dir / "files_index.json") as f:
            files_index = json.load(f)
    except:
        files_index = []
    
    try:
        with open(artifacts_dir / "database_context.json") as f:
            database_context = json.load(f)
    except:
        database_context = {}
    
    try:
        with open(artifacts_dir / "sql_complexity_analysis.json") as f:
            sql_complexity = json.load(f)
    except:
        sql_complexity = {}
    
    try:
        with open(artifacts_dir / "findings.json") as f:
            findings = json.load(f)
    except:
        findings = {}
    
    try:
        with open(artifacts_dir / "repo_summary.json") as f:
            repo_summary = json.load(f)
    except:
        repo_summary = {}
    
    try:
        with open(artifacts_dir / "complexity.json") as f:
            complexity = json.load(f)
    except:
        complexity = {}
    
    try:
        with open(artifacts_dir / "unresolved.json") as f:
            unresolved_vars = json.load(f)
    except:
        unresolved_vars = []
    
    try:
        with open(artifacts_dir / "partially_resolved.json") as f:
            partial_vars = json.load(f)
    except:
        partial_vars = []
    
    try:
        with open(artifacts_dir / "variables.json") as f:
            variables_data = json.load(f)
    except:
        variables_data = None
    
    # Load additional data for gaps export
    try:
        with open(artifacts_dir / "findings.json") as f:
            findings_data = json.load(f)
    except:
        findings_data = {}
    
    # Export each type
    if files_index:
        count = export_files_inventory(files_index, output_dir / "1_files_inventory.csv")
        results['files_inventory'] = count
    
    if database_context:
        count = export_database_tables(database_context, output_dir / "2_database_tables.csv")
        results['database_tables'] = count
        
        count = export_variables(database_context, output_dir / "5_variables.csv")
        results['variables'] = count
    
    if sql_complexity:
        count = export_sql_complexity(sql_complexity, output_dir / "3_sql_complexity.csv")
        results['sql_complexity'] = count
    
    if findings:
        count = export_connections(findings, output_dir / "4_connections.csv")
        results['connections'] = count
    
    # Always export summary
    count = export_master_summary(repo_summary, database_context, sql_complexity, complexity, output_dir / "0_master_summary.csv")
    results['master_summary'] = count
    
    # Export gaps (file name + identified gaps)
    try:
        count = export_gaps(
            files_index,
            unresolved_vars,
            partial_vars,
            sql_complexity,
            complexity,
            variables_data=variables_data,
            database_context=database_context,
            findings=findings_data,
            output_path=output_dir / "6_gaps.csv"
        )
        results['gaps'] = count
    except Exception as e:
        # Log error but don't fail the entire export
        import sys
        import traceback
        print(f"Warning: Error exporting gaps CSV: {e}", file=sys.stderr)
        print(traceback.format_exc(), file=sys.stderr)
        results['gaps'] = 0
    
    return results


def export_csv_from_run_dir(run_dir: Path) -> Path:
    """
    Export CSV files from a run directory.
    
    Args:
        run_dir: Path to run directory (e.g., output_files/run_20240101_120000)
        
    Returns:
        Path to CSV export directory
    """
    artifacts_dir = run_dir / "artifacts"
    csv_dir = run_dir / "csv_exports"
    
    results = export_all_to_csv(artifacts_dir, csv_dir)
    
    # Create README (explicit UTF-8 to support Unicode like →)
    readme_path = csv_dir / "README.txt"
    with open(readme_path, 'w', encoding='utf-8') as f:
        f.write("CSV Export - Cloudera \u2192 Databricks Migration Analysis\n")
        f.write("=" * 60 + "\n\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write("Files:\n")
        f.write("------\n")
        f.write("0_master_summary.csv     - High-level summary metrics\n")
        f.write(f"1_files_inventory.csv    - Complete file listing ({results.get('files_inventory', 0)} files)\n")
        f.write(f"2_database_tables.csv    - All table references ({results.get('database_tables', 0)} tables)\n")
        f.write(f"3_sql_complexity.csv     - SQL complexity analysis ({results.get('sql_complexity', 0)} queries)\n")
        f.write(f"4_connections.csv        - JDBC/URLs/Kafka/Storage ({results.get('connections', 0)} items)\n")
        f.write(f"5_variables.csv          - All variables found ({results.get('variables', 0)} variables)\n\n")
        f.write("Usage:\n")
        f.write("------\n")
        f.write("- Open in Excel for analysis\n")
        f.write("- Import into database for querying\n")
        f.write("- Use in data analysis tools (Python, R, etc.)\n")
        f.write("- Share with stakeholders\n")
    
    return csv_dir


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        run_dir = Path(sys.argv[1])
        if run_dir.exists():
            csv_dir = export_csv_from_run_dir(run_dir)
            print(f"✅ CSV exports generated: {csv_dir}")
            print(f"\nFiles created:")
            for f in sorted(csv_dir.glob("*.csv")):
                print(f"  - {f.name}")
        else:
            print(f"❌ Directory not found: {run_dir}")
    else:
        print("Usage: python csv_export.py /path/to/run_directory")
