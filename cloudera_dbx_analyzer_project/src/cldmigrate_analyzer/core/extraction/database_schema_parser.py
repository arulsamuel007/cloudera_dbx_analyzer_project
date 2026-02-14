"""
Database and Schema Extraction Module - FIXED VERSION

This module provides enhanced parsing of database/schema information from SQL files,
with proper handling of:
- IF NOT EXISTS clauses
- Hive variables (${hiveconf:var}, ${var})
- Complex qualified names
- Multiple database notations
"""

import re
from typing import Dict, List, Any, Optional, Set, Tuple
from dataclasses import dataclass, asdict
from pathlib import Path


@dataclass
class TableReference:
    """Represents a parsed table reference with database/schema context"""
    full_name: str              # Complete reference as found (e.g., "db.schema.table")
    database: Optional[str]     # Database name (if present)
    schema: Optional[str]       # Schema name (if present, or same as database)
    table: str                  # Table name
    operation: str              # Operation type: SELECT, INSERT, UPDATE, DELETE, CREATE, etc.
    line_number: int           # Line where reference was found
    confidence: str            # high, medium, low
    has_variables: bool        # True if contains ${...} placeholders
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class DatabaseContext:
    """Represents database context information from a SQL file"""
    databases: List[str]                    # All databases referenced
    schemas: List[str]                      # All schemas referenced (may overlap with databases)
    use_statements: List[Dict[str, Any]]   # USE database statements
    source_tables: List[TableReference]     # Tables being read from
    target_tables: List[TableReference]     # Tables being written to
    qualified_tables: List[Dict[str, str]]  # All qualified table references
    unqualified_tables: List[str]           # Tables without db/schema prefix
    active_database: Optional[str]          # Last USE statement database (if any)
    variables_found: List[str]              # All Hive variables found
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "databases": self.databases,
            "schemas": self.schemas,
            "use_statements": self.use_statements,
            "source_tables": [t.to_dict() for t in self.source_tables],
            "target_tables": [t.to_dict() for t in self.target_tables],
            "qualified_tables": self.qualified_tables,
            "unqualified_tables": self.unqualified_tables,
            "active_database": self.active_database,
            "variables_found": self.variables_found,
            "summary": {
                "total_databases": len(self.databases),
                "total_schemas": len(self.schemas),
                "total_source_tables": len(self.source_tables),
                "total_target_tables": len(self.target_tables),
                "total_qualified_refs": len(self.qualified_tables),
                "total_unqualified_refs": len(self.unqualified_tables),
                "total_variables": len(self.variables_found)
            }
        }


class DatabaseSchemaParser:
    """
    Enhanced parser for database and schema information from SQL text.
    
    FIXED ISSUES:
    - Properly handles IF NOT EXISTS in CREATE TABLE statements
    - Correctly extracts Hive variables (${hiveconf:var}, ${var})
    - Parses qualified table names with variables
    """
    
    # Regex patterns for SQL constructs
    USE_DB_RE = re.compile(
        r'(?i)\bUSE\s+(?:DATABASE\s+)?(?:SCHEMA\s+)?([`"\[]?[\w]+[`"\]]?)\s*;?',
        re.MULTILINE
    )
    
    # FIXED: Updated CREATE TABLE pattern to handle IF NOT EXISTS
    SQL_CREATE_RE = re.compile(
        r'(?i)\bCREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?'
        r'([`"\[]?[\w\.\$\{\}:]+[`"\]]?)',
        re.MULTILINE
    )
    
    # Updated patterns for other operations
    SQL_FROM_RE = re.compile(
        r'(?i)\bFROM\s+([`"\[]?[\w\.\$\{\}:]+[`"\]]?)',
        re.MULTILINE
    )
    
    SQL_JOIN_RE = re.compile(
        r'(?i)\b(?:INNER\s+|LEFT\s+(?:OUTER\s+)?|RIGHT\s+(?:OUTER\s+)?|FULL\s+(?:OUTER\s+)?|CROSS\s+)?JOIN\s+'
        r'([`"\[]?[\w\.\$\{\}:]+[`"\]]?)',
        re.MULTILINE
    )
    
    SQL_INSERT_RE = re.compile(
        r'(?i)\bINSERT\s+(?:INTO|OVERWRITE)\s+(?:TABLE\s+)?'
        r'([`"\[]?[\w\.\$\{\}:]+[`"\]]?)',
        re.MULTILINE
    )
    
    SQL_MERGE_RE = re.compile(
        r'(?i)\bMERGE\s+INTO\s+([`"\[]?[\w\.\$\{\}:]+[`"\]]?)',
        re.MULTILINE
    )
    
    SQL_UPDATE_RE = re.compile(
        r'(?i)\bUPDATE\s+([`"\[]?[\w\.\$\{\}:]+[`"\]]?)',
        re.MULTILINE
    )
    
    SQL_DELETE_RE = re.compile(
        r'(?i)\bDELETE\s+FROM\s+([`"\[]?[\w\.\$\{\}:]+[`"\]]?)',
        re.MULTILINE
    )
    
    SQL_TRUNCATE_RE = re.compile(
        r'(?i)\bTRUNCATE\s+(?:TABLE\s+)?([`"\[]?[\w\.\$\{\}:]+[`"\]]?)',
        re.MULTILINE
    )
    
    # Hive variable patterns - ENHANCED
    HIVECONF_VAR_RE = re.compile(r'\$\{hiveconf:(\w+)\}')
    HIVEVAR_VAR_RE = re.compile(r'\$\{hivevar:(\w+)\}')
    SIMPLE_VAR_RE = re.compile(r'\$\{(\w+)\}')
    ALL_VAR_RE = re.compile(r'\$\{[^}]+\}')
    
    @staticmethod
    def clean_identifier(name: str) -> str:
        """Remove backticks, quotes, and brackets from identifiers"""
        return name.strip('`"[]').strip()
    
    @staticmethod
    def extract_variables(text: str) -> List[str]:
        """Extract all Hive variables from text"""
        variables = set()
        
        # Extract hiveconf variables
        for match in DatabaseSchemaParser.HIVECONF_VAR_RE.finditer(text):
            variables.add(f"hiveconf:{match.group(1)}")
        
        # Extract hivevar variables
        for match in DatabaseSchemaParser.HIVEVAR_VAR_RE.finditer(text):
            variables.add(f"hivevar:{match.group(1)}")
        
        # Extract simple variables (not hiveconf or hivevar)
        for match in DatabaseSchemaParser.SIMPLE_VAR_RE.finditer(text):
            full_match = match.group(0)
            if 'hiveconf:' not in full_match and 'hivevar:' not in full_match:
                variables.add(match.group(1))
        
        return sorted(list(variables))
    
    @staticmethod
    def parse_qualified_name(full_name: str) -> Tuple[Optional[str], Optional[str], str]:
        """
        Parse a qualified table name into components, handling Hive variables.
        
        Examples:
        - "customers" → (None, None, "customers")
        - "sales.customers" → ("sales", None, "customers")
        - "${hiveconf:raw_db}.raw_customer" → ("${hiveconf:raw_db}", None, "raw_customer")
        - "prod.sales.customers" → ("prod", "sales", "customers")
        
        Returns: (database/catalog, schema, table)
        """
        # Check if it contains variables
        has_vars = bool(DatabaseSchemaParser.ALL_VAR_RE.search(full_name))
        
        # Split by dots, preserving variable expressions
        parts = []
        current = ""
        in_variable = False
        
        for char in full_name:
            if char == '$' and len(full_name) > full_name.index('$') + 1 and full_name[full_name.index('$') + 1] == '{':
                in_variable = True
                current += char
            elif char == '}' and in_variable:
                in_variable = False
                current += char
            elif char == '.' and not in_variable:
                if current:
                    parts.append(DatabaseSchemaParser.clean_identifier(current))
                    current = ""
            else:
                current += char
        
        if current:
            parts.append(DatabaseSchemaParser.clean_identifier(current))
        
        # Parse based on number of parts
        if len(parts) == 1:
            return (None, None, parts[0])
        elif len(parts) == 2:
            return (parts[0], None, parts[1])
        elif len(parts) == 3:
            return (parts[0], parts[1], parts[2])
        else:
            # Malformed - return as-is
            return (None, None, full_name)
    
    @staticmethod
    def extract_use_statements(text: str) -> List[Dict[str, Any]]:
        """Extract all USE database/schema statements"""
        use_stmts = []
        for i, line in enumerate(text.split('\n'), 1):
            for match in DatabaseSchemaParser.USE_DB_RE.finditer(line):
                db_name = DatabaseSchemaParser.clean_identifier(match.group(1))
                use_stmts.append({
                    "database": db_name,
                    "line": i,
                    "statement": match.group(0).strip()
                })
        return use_stmts
    
    @staticmethod
    def extract_table_references(
        text: str,
        operation: str,
        regex_pattern: re.Pattern,
        active_database: Optional[str] = None
    ) -> List[TableReference]:
        """
        Extract table references from SQL using the given pattern.
        
        Args:
            text: SQL text to parse
            operation: Operation type (SELECT, INSERT, etc.)
            regex_pattern: Compiled regex to find tables
            active_database: Current active database from USE statement
        """
        refs = []
        lines = text.split('\n')
        
        for i, line in enumerate(lines, 1):
            for match in regex_pattern.finditer(line):
                full_name = DatabaseSchemaParser.clean_identifier(match.group(1))
                
                # Skip if this looks like a keyword or SQL construct
                if full_name.upper() in ('IF', 'NOT', 'EXISTS', 'EXTERNAL', 'TABLE'):
                    continue
                
                # Check for variables
                has_vars = bool(DatabaseSchemaParser.ALL_VAR_RE.search(full_name))
                
                # Parse qualified name
                database, schema, table = DatabaseSchemaParser.parse_qualified_name(full_name)
                
                # If no database specified but we have an active database, use it
                if not database and active_database and not has_vars:
                    database = active_database
                    schema = None
                
                # Determine confidence
                if has_vars:
                    confidence = "low" if database and DatabaseSchemaParser.ALL_VAR_RE.search(database) else "medium"
                elif database or schema:
                    confidence = "high"
                else:
                    confidence = "medium"
                
                ref = TableReference(
                    full_name=full_name,
                    database=database,
                    schema=schema,
                    table=table,
                    operation=operation,
                    line_number=i,
                    confidence=confidence,
                    has_variables=has_vars
                )
                refs.append(ref)
        
        return refs
    
    @staticmethod
    def extract_databases_and_schemas(text: str) -> DatabaseContext:
        """
        Main extraction function that returns comprehensive database/schema context.
        
        Args:
            text: SQL text to analyze
            
        Returns:
            DatabaseContext object with all extracted information
        """
        # Extract USE statements first to establish context
        use_stmts = DatabaseSchemaParser.extract_use_statements(text)
        active_database = use_stmts[-1]["database"] if use_stmts else None
        
        # Extract all variables
        all_variables = DatabaseSchemaParser.extract_variables(text)
        
        # Extract all table references by operation type
        source_refs = []
        target_refs = []
        
        # Source tables (reads)
        source_refs.extend(
            DatabaseSchemaParser.extract_table_references(
                text, "SELECT", DatabaseSchemaParser.SQL_FROM_RE, active_database
            )
        )
        source_refs.extend(
            DatabaseSchemaParser.extract_table_references(
                text, "JOIN", DatabaseSchemaParser.SQL_JOIN_RE, active_database
            )
        )
        
        # Target tables (writes)
        target_refs.extend(
            DatabaseSchemaParser.extract_table_references(
                text, "INSERT", DatabaseSchemaParser.SQL_INSERT_RE, active_database
            )
        )
        target_refs.extend(
            DatabaseSchemaParser.extract_table_references(
                text, "CREATE", DatabaseSchemaParser.SQL_CREATE_RE, active_database
            )
        )
        target_refs.extend(
            DatabaseSchemaParser.extract_table_references(
                text, "MERGE", DatabaseSchemaParser.SQL_MERGE_RE, active_database
            )
        )
        target_refs.extend(
            DatabaseSchemaParser.extract_table_references(
                text, "UPDATE", DatabaseSchemaParser.SQL_UPDATE_RE, active_database
            )
        )
        target_refs.extend(
            DatabaseSchemaParser.extract_table_references(
                text, "DELETE", DatabaseSchemaParser.SQL_DELETE_RE, active_database
            )
        )
        target_refs.extend(
            DatabaseSchemaParser.extract_table_references(
                text, "TRUNCATE", DatabaseSchemaParser.SQL_TRUNCATE_RE, active_database
            )
        )
        
        # Collect unique databases and schemas
        databases: Set[str] = set()
        schemas: Set[str] = set()
        qualified_tables: List[Dict[str, str]] = []
        unqualified_tables: Set[str] = set()
        
        for ref in source_refs + target_refs:
            # Only add non-variable databases
            if ref.database and not DatabaseSchemaParser.ALL_VAR_RE.search(ref.database):
                databases.add(ref.database)
            if ref.schema:
                schemas.add(ref.schema)
            
            if ref.database or ref.schema:
                qualified_tables.append({
                    "full_name": ref.full_name,
                    "database": ref.database,
                    "schema": ref.schema,
                    "table": ref.table
                })
            else:
                unqualified_tables.add(ref.table)
        
        # Add databases from USE statements
        for use_stmt in use_stmts:
            databases.add(use_stmt["database"])
        
        return DatabaseContext(
            databases=sorted(list(databases)),
            schemas=sorted(list(schemas)),
            use_statements=use_stmts,
            source_tables=source_refs,
            target_tables=target_refs,
            qualified_tables=qualified_tables,
            unqualified_tables=sorted(list(unqualified_tables)),
            active_database=active_database,
            variables_found=all_variables
        )


def extract_databases_from_repository(
    repo_root: Path,
    files_index: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Repository-level database/schema extraction.
    
    Scans all SQL files and aggregates database/schema information across the entire repo.
    """
    all_databases: Set[str] = set()
    all_schemas: Set[str] = set()
    all_source_tables: List[Dict[str, Any]] = []
    all_target_tables: List[Dict[str, Any]] = []
    all_variables: Set[str] = set()
    files_by_database: Dict[str, List[str]] = {}
    tables_by_database: Dict[str, Set[str]] = {}
    
    for file_info in files_index:
        rel_path = file_info.get("path")
        if not rel_path:
            continue
        
        # Only process SQL-like files
        file_type = (file_info.get("detected_type") or "").lower()
        if file_type not in {
            "sql", "hql", "impala_sql", "oozie_workflow_xml",
            "oozie_coordinator_xml", "notebook_zeppelin", "notebook_jupyter"
        }:
            continue
        
        file_path = repo_root / rel_path
        try:
            text = file_path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        
        # Extract context for this file
        context = DatabaseSchemaParser.extract_databases_and_schemas(text)
        
        # Aggregate databases and schemas
        all_databases.update(context.databases)
        all_schemas.update(context.schemas)
        all_variables.update(context.variables_found)
        
        # Track which files use which databases
        for db in context.databases:
            if db not in files_by_database:
                files_by_database[db] = []
            files_by_database[db].append(rel_path)
        
        # Add file reference to each table reference
        for src_table in context.source_tables:
            table_dict = src_table.to_dict()
            table_dict["file"] = rel_path
            all_source_tables.append(table_dict)
            
            # Track tables by database (only non-variable databases)
            if src_table.database and not DatabaseSchemaParser.ALL_VAR_RE.search(src_table.database):
                if src_table.database not in tables_by_database:
                    tables_by_database[src_table.database] = set()
                tables_by_database[src_table.database].add(src_table.table)
        
        for tgt_table in context.target_tables:
            table_dict = tgt_table.to_dict()
            table_dict["file"] = rel_path
            all_target_tables.append(table_dict)
            
            # Track tables by database (only non-variable databases)
            if tgt_table.database and not DatabaseSchemaParser.ALL_VAR_RE.search(tgt_table.database):
                if tgt_table.database not in tables_by_database:
                    tables_by_database[tgt_table.database] = set()
                tables_by_database[tgt_table.database].add(tgt_table.table)
    
    # Convert sets to sorted lists for JSON serialization
    tables_by_db_list = {
        db: sorted(list(tables))
        for db, tables in tables_by_database.items()
    }
    
    return {
        "databases": sorted(list(all_databases)),
        "schemas": sorted(list(all_schemas)),
        "source_tables": all_source_tables,
        "target_tables": all_target_tables,
        "files_by_database": files_by_database,
        "tables_by_database": tables_by_db_list,
        "variables_found": sorted(list(all_variables)),
        "summary": {
            "total_databases": len(all_databases),
            "total_schemas": len(all_schemas),
            "total_source_table_refs": len(all_source_tables),
            "total_target_table_refs": len(all_target_tables),
            "databases_with_tables": len(tables_by_database),
            "total_variables": len(all_variables)
        }
    }
