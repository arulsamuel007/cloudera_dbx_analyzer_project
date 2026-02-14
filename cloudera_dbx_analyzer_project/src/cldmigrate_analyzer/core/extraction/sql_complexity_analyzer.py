"""
SQL Complexity Analyzer Module

This module analyzes SQL queries to determine their complexity across multiple dimensions:
- JOIN operations (count, types, complexity)
- Subquery depth and nesting
- CTEs (Common Table Expressions) usage
- Window functions
- Aggregate functions
- Set operations (UNION, INTERSECT, EXCEPT)
- Control structures (CASE WHEN, etc.)
- Table operations (CREATE, ALTER, DROP)

Output is structured for easy reuse and further analysis.
"""

import re
from typing import Dict, List, Any, Optional, Set, Tuple
from dataclasses import dataclass, asdict
from pathlib import Path
from enum import Enum


class ComplexityLevel(Enum):
    """Complexity classification levels"""
    SIMPLE = "simple"           # Score 0-20
    MODERATE = "moderate"       # Score 21-50
    COMPLEX = "complex"         # Score 51-80
    VERY_COMPLEX = "very_complex"  # Score 81+


@dataclass
class JoinAnalysis:
    """Analysis of JOIN operations in a query"""
    total_joins: int
    join_types: Dict[str, int]  # e.g., {"INNER": 2, "LEFT": 1, "CROSS": 1}
    max_tables_joined: int
    has_self_join: bool
    has_cross_join: bool
    join_complexity_score: int
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class SubqueryAnalysis:
    """Analysis of subqueries in a query"""
    total_subqueries: int
    max_nesting_depth: int
    correlated_subqueries: int
    subqueries_in_select: int
    subqueries_in_where: int
    subqueries_in_from: int
    subquery_complexity_score: int
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class CTEAnalysis:
    """Analysis of Common Table Expressions"""
    total_ctes: int
    max_cte_chain_length: int
    recursive_ctes: int
    cte_names: List[str]
    cte_complexity_score: int
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class WindowFunctionAnalysis:
    """Analysis of window/analytic functions"""
    total_window_functions: int
    window_function_types: List[str]  # e.g., ["ROW_NUMBER", "RANK", "LAG"]
    has_partition_by: bool
    has_order_by: bool
    window_complexity_score: int
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class AggregateAnalysis:
    """Analysis of aggregate functions"""
    total_aggregates: int
    aggregate_types: Dict[str, int]  # e.g., {"COUNT": 3, "SUM": 2, "AVG": 1}
    has_group_by: bool
    has_having: bool
    distinct_aggregates: int
    aggregate_complexity_score: int
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class SetOperationAnalysis:
    """Analysis of set operations"""
    total_set_operations: int
    operation_types: Dict[str, int]  # e.g., {"UNION": 2, "INTERSECT": 1}
    has_union_all: bool
    set_operation_complexity_score: int
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ControlStructureAnalysis:
    """Analysis of control structures"""
    total_case_statements: int
    max_case_branches: int
    has_coalesce: bool
    has_nullif: bool
    has_cast: bool
    control_complexity_score: int
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class DDLAnalysis:
    """Analysis of DDL operations"""
    has_create: bool
    has_alter: bool
    has_drop: bool
    has_truncate: bool
    partition_operations: int
    index_operations: int
    ddl_complexity_score: int
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class SQLComplexityResult:
    """Complete SQL complexity analysis result"""
    file_path: str
    line_number: int
    query_snippet: str  # First 200 chars
    query_length: int  # Total characters
    query_lines: int
    
    # Component analyses
    join_analysis: JoinAnalysis
    subquery_analysis: SubqueryAnalysis
    cte_analysis: CTEAnalysis
    window_function_analysis: WindowFunctionAnalysis
    aggregate_analysis: AggregateAnalysis
    set_operation_analysis: SetOperationAnalysis
    control_structure_analysis: ControlStructureAnalysis
    ddl_analysis: DDLAnalysis
    
    # Overall scores
    total_complexity_score: int
    complexity_level: str
    risk_flags: List[str]
    
    # Additional metadata
    has_dynamic_sql: bool
    has_nested_views: bool
    estimated_execution_complexity: str  # low, medium, high, very_high
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "file_path": self.file_path,
            "line_number": self.line_number,
            "query_snippet": self.query_snippet,
            "query_length": self.query_length,
            "query_lines": self.query_lines,
            "join_analysis": self.join_analysis.to_dict(),
            "subquery_analysis": self.subquery_analysis.to_dict(),
            "cte_analysis": self.cte_analysis.to_dict(),
            "window_function_analysis": self.window_function_analysis.to_dict(),
            "aggregate_analysis": self.aggregate_analysis.to_dict(),
            "set_operation_analysis": self.set_operation_analysis.to_dict(),
            "control_structure_analysis": self.control_structure_analysis.to_dict(),
            "ddl_analysis": self.ddl_analysis.to_dict(),
            "total_complexity_score": self.total_complexity_score,
            "complexity_level": self.complexity_level,
            "risk_flags": self.risk_flags,
            "has_dynamic_sql": self.has_dynamic_sql,
            "has_nested_views": self.has_nested_views,
            "estimated_execution_complexity": self.estimated_execution_complexity
        }


class SQLComplexityAnalyzer:
    """
    Analyzes SQL queries to determine their complexity.
    
    Usage:
        analyzer = SQLComplexityAnalyzer()
        result = analyzer.analyze_query(sql_text, "path/to/file.sql", line_number=10)
    """
    
    # Regex patterns for SQL constructs
    JOIN_PATTERN = re.compile(
        r'\b(INNER\s+JOIN|LEFT\s+(?:OUTER\s+)?JOIN|RIGHT\s+(?:OUTER\s+)?JOIN|'
        r'FULL\s+(?:OUTER\s+)?JOIN|CROSS\s+JOIN|JOIN)\b',
        re.IGNORECASE
    )
    
    SUBQUERY_PATTERN = re.compile(
        r'\(\s*SELECT\b',
        re.IGNORECASE
    )
    
    CTE_PATTERN = re.compile(
        r'\bWITH\s+(\w+)\s+AS\s*\(',
        re.IGNORECASE
    )
    
    RECURSIVE_CTE_PATTERN = re.compile(
        r'\bWITH\s+RECURSIVE\b',
        re.IGNORECASE
    )
    
    WINDOW_FUNCTIONS = [
        'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'NTILE', 'LAG', 'LEAD',
        'FIRST_VALUE', 'LAST_VALUE', 'NTH_VALUE', 'PERCENT_RANK',
        'CUME_DIST', 'PERCENTILE_CONT', 'PERCENTILE_DISC'
    ]
    
    AGGREGATE_FUNCTIONS = [
        'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'STDDEV', 'VARIANCE',
        'STRING_AGG', 'ARRAY_AGG', 'LISTAGG', 'GROUP_CONCAT'
    ]
    
    SET_OPERATIONS = ['UNION', 'INTERSECT', 'EXCEPT', 'MINUS']
    
    def __init__(self):
        """Initialize the SQL complexity analyzer"""
        self.window_pattern = re.compile(
            r'\b(' + '|'.join(self.WINDOW_FUNCTIONS) + r')\s*\(',
            re.IGNORECASE
        )
        self.aggregate_pattern = re.compile(
            r'\b(' + '|'.join(self.AGGREGATE_FUNCTIONS) + r')\s*\(',
            re.IGNORECASE
        )
    
    def analyze_query(
        self,
        sql_text: str,
        file_path: str = "unknown",
        line_number: int = 0
    ) -> SQLComplexityResult:
        """
        Analyze a SQL query and return comprehensive complexity metrics.
        
        Args:
            sql_text: The SQL query text to analyze
            file_path: Path to the file containing the query
            line_number: Line number where the query starts
            
        Returns:
            SQLComplexityResult object with all complexity metrics
        """
        # Clean and normalize SQL
        sql_clean = self._normalize_sql(sql_text)
        
        # Perform individual analyses
        join_analysis = self._analyze_joins(sql_clean)
        subquery_analysis = self._analyze_subqueries(sql_clean, sql_text)
        cte_analysis = self._analyze_ctes(sql_clean)
        window_analysis = self._analyze_window_functions(sql_clean)
        aggregate_analysis = self._analyze_aggregates(sql_clean)
        set_op_analysis = self._analyze_set_operations(sql_clean)
        control_analysis = self._analyze_control_structures(sql_clean)
        ddl_analysis = self._analyze_ddl(sql_clean)
        
        # Calculate total complexity score
        total_score = (
            join_analysis.join_complexity_score +
            subquery_analysis.subquery_complexity_score +
            cte_analysis.cte_complexity_score +
            window_analysis.window_complexity_score +
            aggregate_analysis.aggregate_complexity_score +
            set_op_analysis.set_operation_complexity_score +
            control_analysis.control_complexity_score +
            ddl_analysis.ddl_complexity_score
        )
        
        # Determine complexity level
        complexity_level = self._classify_complexity(total_score)
        
        # Identify risk flags
        risk_flags = self._identify_risk_flags(
            join_analysis, subquery_analysis, cte_analysis,
            window_analysis, aggregate_analysis, set_op_analysis
        )
        
        # Estimate execution complexity
        exec_complexity = self._estimate_execution_complexity(
            join_analysis, subquery_analysis, total_score
        )
        
        # Check for special patterns
        has_dynamic_sql = self._has_dynamic_sql(sql_text)
        has_nested_views = self._has_nested_views(sql_clean)
        
        # Create result object
        return SQLComplexityResult(
            file_path=file_path,
            line_number=line_number,
            query_snippet=sql_text[:200].replace('\n', ' '),
            query_length=len(sql_text),
            query_lines=len(sql_text.split('\n')),
            join_analysis=join_analysis,
            subquery_analysis=subquery_analysis,
            cte_analysis=cte_analysis,
            window_function_analysis=window_analysis,
            aggregate_analysis=aggregate_analysis,
            set_operation_analysis=set_op_analysis,
            control_structure_analysis=control_analysis,
            ddl_analysis=ddl_analysis,
            total_complexity_score=total_score,
            complexity_level=complexity_level.value,
            risk_flags=risk_flags,
            has_dynamic_sql=has_dynamic_sql,
            has_nested_views=has_nested_views,
            estimated_execution_complexity=exec_complexity
        )
    
    def _normalize_sql(self, sql: str) -> str:
        """Remove comments and normalize whitespace"""
        # Remove single-line comments
        sql = re.sub(r'--[^\n]*', '', sql)
        # Remove multi-line comments
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
        # Normalize whitespace
        sql = ' '.join(sql.split())
        return sql
    
    def _analyze_joins(self, sql: str) -> JoinAnalysis:
        """Analyze JOIN operations"""
        joins = self.JOIN_PATTERN.findall(sql)
        total_joins = len(joins)
        
        # Count join types
        join_types = {}
        for join in joins:
            join_type = join.strip().replace('OUTER', '').strip()
            join_types[join_type] = join_types.get(join_type, 0) + 1
        
        # Estimate max tables joined (rough heuristic)
        max_tables = total_joins + 1 if total_joins > 0 else 1
        
        # Check for special join types
        has_self_join = self._detect_self_join(sql)
        has_cross_join = 'CROSS JOIN' in sql.upper()
        
        # Calculate complexity score
        score = 0
        score += total_joins * 5  # Base points per join
        score += join_types.get('CROSS JOIN', 0) * 15  # Cross joins are expensive
        score += 10 if has_self_join else 0
        score += max(0, (total_joins - 3) * 3)  # Penalty for many joins
        
        return JoinAnalysis(
            total_joins=total_joins,
            join_types=join_types,
            max_tables_joined=max_tables,
            has_self_join=has_self_join,
            has_cross_join=has_cross_join,
            join_complexity_score=score
        )
    
    def _analyze_subqueries(self, sql_clean: str, sql_original: str) -> SubqueryAnalysis:
        """Analyze subquery usage and nesting"""
        subqueries = self.SUBQUERY_PATTERN.findall(sql_clean)
        total_subqueries = len(subqueries)
        
        # Calculate nesting depth
        max_depth = self._calculate_subquery_depth(sql_original)
        
        # Detect correlated subqueries (rough heuristic)
        correlated = len(re.findall(r'WHERE.*?=.*?\(.*?SELECT', sql_clean, re.IGNORECASE | re.DOTALL))
        
        # Count subqueries in different clauses
        in_select = len(re.findall(r'SELECT[^(]*\([^)]*SELECT', sql_clean, re.IGNORECASE))
        in_where = len(re.findall(r'WHERE[^(]*\([^)]*SELECT', sql_clean, re.IGNORECASE))
        in_from = len(re.findall(r'FROM[^(]*\([^)]*SELECT', sql_clean, re.IGNORECASE))
        
        # Calculate complexity score
        score = 0
        score += total_subqueries * 8
        score += max_depth * 10
        score += correlated * 12  # Correlated subqueries are expensive
        
        return SubqueryAnalysis(
            total_subqueries=total_subqueries,
            max_nesting_depth=max_depth,
            correlated_subqueries=correlated,
            subqueries_in_select=in_select,
            subqueries_in_where=in_where,
            subqueries_in_from=in_from,
            subquery_complexity_score=score
        )
    
    def _analyze_ctes(self, sql: str) -> CTEAnalysis:
        """Analyze Common Table Expressions"""
        cte_matches = self.CTE_PATTERN.findall(sql)
        total_ctes = len(cte_matches)
        cte_names = cte_matches
        
        # Detect recursive CTEs
        recursive_ctes = len(self.RECURSIVE_CTE_PATTERN.findall(sql))
        
        # Estimate CTE chain length (rough heuristic)
        max_chain = min(total_ctes, 5)  # Conservative estimate
        
        # Calculate complexity score
        score = 0
        score += total_ctes * 5
        score += recursive_ctes * 20  # Recursive CTEs are complex
        score += max(0, (total_ctes - 2) * 3)  # Penalty for many CTEs
        
        return CTEAnalysis(
            total_ctes=total_ctes,
            max_cte_chain_length=max_chain,
            recursive_ctes=recursive_ctes,
            cte_names=cte_names,
            cte_complexity_score=score
        )
    
    def _analyze_window_functions(self, sql: str) -> WindowFunctionAnalysis:
        """Analyze window/analytic functions"""
        window_matches = self.window_pattern.findall(sql)
        total_windows = len(window_matches)
        window_types = list(set(match.upper() for match in window_matches))
        
        has_partition = bool(re.search(r'\bPARTITION\s+BY\b', sql, re.IGNORECASE))
        has_order = bool(re.search(r'\bORDER\s+BY\b', sql, re.IGNORECASE))
        
        # Calculate complexity score
        score = 0
        score += total_windows * 10  # Window functions are moderately expensive
        score += 5 if has_partition else 0
        score += 3 if has_order else 0
        
        return WindowFunctionAnalysis(
            total_window_functions=total_windows,
            window_function_types=window_types,
            has_partition_by=has_partition,
            has_order_by=has_order,
            window_complexity_score=score
        )
    
    def _analyze_aggregates(self, sql: str) -> AggregateAnalysis:
        """Analyze aggregate functions"""
        agg_matches = self.aggregate_pattern.findall(sql)
        total_aggs = len(agg_matches)
        
        # Count by type
        agg_types = {}
        for agg in agg_matches:
            agg_upper = agg.upper()
            agg_types[agg_upper] = agg_types.get(agg_upper, 0) + 1
        
        has_group_by = bool(re.search(r'\bGROUP\s+BY\b', sql, re.IGNORECASE))
        has_having = bool(re.search(r'\bHAVING\b', sql, re.IGNORECASE))
        distinct_aggs = len(re.findall(r'\b(COUNT|SUM|AVG)\s*\(\s*DISTINCT', sql, re.IGNORECASE))
        
        # Calculate complexity score
        score = 0
        score += total_aggs * 3
        score += 5 if has_group_by else 0
        score += 5 if has_having else 0
        score += distinct_aggs * 5
        
        return AggregateAnalysis(
            total_aggregates=total_aggs,
            aggregate_types=agg_types,
            has_group_by=has_group_by,
            has_having=has_having,
            distinct_aggregates=distinct_aggs,
            aggregate_complexity_score=score
        )
    
    def _analyze_set_operations(self, sql: str) -> SetOperationAnalysis:
        """Analyze set operations"""
        set_ops = {}
        total = 0
        
        for op in self.SET_OPERATIONS:
            count = len(re.findall(rf'\b{op}\b', sql, re.IGNORECASE))
            if count > 0:
                set_ops[op] = count
                total += count
        
        has_union_all = bool(re.search(r'\bUNION\s+ALL\b', sql, re.IGNORECASE))
        
        # Calculate complexity score
        score = total * 8
        
        return SetOperationAnalysis(
            total_set_operations=total,
            operation_types=set_ops,
            has_union_all=has_union_all,
            set_operation_complexity_score=score
        )
    
    def _analyze_control_structures(self, sql: str) -> ControlStructureAnalysis:
        """Analyze control structures"""
        case_statements = len(re.findall(r'\bCASE\b', sql, re.IGNORECASE))
        
        # Find max branches in CASE statements
        max_branches = 0
        case_blocks = re.findall(r'CASE.*?END', sql, re.IGNORECASE | re.DOTALL)
        for block in case_blocks:
            when_count = len(re.findall(r'\bWHEN\b', block, re.IGNORECASE))
            max_branches = max(max_branches, when_count)
        
        has_coalesce = bool(re.search(r'\bCOALESCE\s*\(', sql, re.IGNORECASE))
        has_nullif = bool(re.search(r'\bNULLIF\s*\(', sql, re.IGNORECASE))
        has_cast = bool(re.search(r'\bCAST\s*\(', sql, re.IGNORECASE))
        
        # Calculate complexity score
        score = 0
        score += case_statements * 5
        score += max(0, (max_branches - 3) * 2)  # Penalty for many branches
        score += 2 if has_coalesce else 0
        score += 2 if has_nullif else 0
        score += 1 if has_cast else 0
        
        return ControlStructureAnalysis(
            total_case_statements=case_statements,
            max_case_branches=max_branches,
            has_coalesce=has_coalesce,
            has_nullif=has_nullif,
            has_cast=has_cast,
            control_complexity_score=score
        )
    
    def _analyze_ddl(self, sql: str) -> DDLAnalysis:
        """Analyze DDL operations"""
        has_create = bool(re.search(r'\bCREATE\s+(TABLE|VIEW|INDEX)', sql, re.IGNORECASE))
        has_alter = bool(re.search(r'\bALTER\s+TABLE', sql, re.IGNORECASE))
        has_drop = bool(re.search(r'\bDROP\s+(TABLE|VIEW|INDEX)', sql, re.IGNORECASE))
        has_truncate = bool(re.search(r'\bTRUNCATE\s+TABLE', sql, re.IGNORECASE))
        
        partition_ops = len(re.findall(r'\bPARTITION(ED)?\s+BY\b', sql, re.IGNORECASE))
        index_ops = len(re.findall(r'\bINDEX\b', sql, re.IGNORECASE))
        
        # Calculate complexity score
        score = 0
        score += 10 if has_create else 0
        score += 8 if has_alter else 0
        score += 5 if has_drop else 0
        score += 5 if has_truncate else 0
        score += partition_ops * 5
        score += index_ops * 3
        
        return DDLAnalysis(
            has_create=has_create,
            has_alter=has_alter,
            has_drop=has_drop,
            has_truncate=has_truncate,
            partition_operations=partition_ops,
            index_operations=index_ops,
            ddl_complexity_score=score
        )
    
    def _calculate_subquery_depth(self, sql: str) -> int:
        """Calculate maximum subquery nesting depth"""
        max_depth = 0
        current_depth = 0
        
        for char in sql:
            if char == '(':
                current_depth += 1
                max_depth = max(max_depth, current_depth)
            elif char == ')':
                current_depth = max(0, current_depth - 1)
        
        # Rough adjustment for SELECT depth
        return min(max_depth // 2, 10)  # Cap at 10
    
    def _detect_self_join(self, sql: str) -> bool:
        """Detect if query contains self-joins"""
        # Very rough heuristic - look for same table aliased twice
        from_matches = re.findall(r'FROM\s+(\w+)\s+(\w+)', sql, re.IGNORECASE)
        join_matches = re.findall(r'JOIN\s+(\w+)\s+(\w+)', sql, re.IGNORECASE)
        
        all_tables = [m[0].upper() for m in from_matches + join_matches]
        return len(all_tables) != len(set(all_tables))
    
    def _has_dynamic_sql(self, sql: str) -> bool:
        """Check for dynamic SQL patterns"""
        patterns = [
            r'EXECUTE\s+IMMEDIATE',
            r'EXEC\s*\(',
            r'sp_executesql',
            r'\|\|.*SELECT',  # String concatenation with SELECT
            r'\+.*SELECT',     # String concatenation with SELECT
        ]
        return any(re.search(p, sql, re.IGNORECASE) for p in patterns)
    
    def _has_nested_views(self, sql: str) -> bool:
        """Check if query references views (rough heuristic)"""
        return bool(re.search(r'FROM\s+\w*[vV]iew\w*', sql, re.IGNORECASE))
    
    def _classify_complexity(self, score: int) -> ComplexityLevel:
        """Classify complexity based on total score"""
        if score <= 20:
            return ComplexityLevel.SIMPLE
        elif score <= 50:
            return ComplexityLevel.MODERATE
        elif score <= 80:
            return ComplexityLevel.COMPLEX
        else:
            return ComplexityLevel.VERY_COMPLEX
    
    def _identify_risk_flags(
        self,
        join_analysis: JoinAnalysis,
        subquery_analysis: SubqueryAnalysis,
        cte_analysis: CTEAnalysis,
        window_analysis: WindowFunctionAnalysis,
        aggregate_analysis: AggregateAnalysis,
        set_op_analysis: SetOperationAnalysis
    ) -> List[str]:
        """Identify specific risk factors"""
        flags = []
        
        if join_analysis.total_joins > 5:
            flags.append("many_joins")
        if join_analysis.has_cross_join:
            flags.append("cross_join")
        if join_analysis.has_self_join:
            flags.append("self_join")
        
        if subquery_analysis.max_nesting_depth > 3:
            flags.append("deep_nesting")
        if subquery_analysis.correlated_subqueries > 0:
            flags.append("correlated_subqueries")
        
        if cte_analysis.recursive_ctes > 0:
            flags.append("recursive_cte")
        if cte_analysis.total_ctes > 5:
            flags.append("many_ctes")
        
        if window_analysis.total_window_functions > 3:
            flags.append("many_window_functions")
        
        if aggregate_analysis.distinct_aggregates > 2:
            flags.append("distinct_aggregates")
        
        if set_op_analysis.total_set_operations > 2:
            flags.append("many_set_operations")
        
        return flags
    
    def _estimate_execution_complexity(
        self,
        join_analysis: JoinAnalysis,
        subquery_analysis: SubqueryAnalysis,
        total_score: int
    ) -> str:
        """Estimate query execution complexity"""
        # High execution complexity factors
        high_factors = 0
        
        if join_analysis.has_cross_join:
            high_factors += 2
        if join_analysis.total_joins > 5:
            high_factors += 1
        if subquery_analysis.correlated_subqueries > 0:
            high_factors += 2
        if subquery_analysis.max_nesting_depth > 3:
            high_factors += 1
        
        if high_factors >= 3 or total_score > 100:
            return "very_high"
        elif high_factors >= 2 or total_score > 60:
            return "high"
        elif total_score > 30:
            return "medium"
        else:
            return "low"


def analyze_sql_file(
    file_path: Path,
    sql_content: str
) -> List[SQLComplexityResult]:
    """
    Analyze all SQL queries in a file.
    
    For simplicity, this function treats the entire file as one query.
    In production, you might want to split by statement terminators.
    """
    analyzer = SQLComplexityAnalyzer()
    
    # For now, analyze the entire file as one query
    # In production, you'd split by semicolons or statement boundaries
    if sql_content.strip():
        result = analyzer.analyze_query(sql_content, str(file_path), line_number=1)
        return [result]
    
    return []


def analyze_repository_sql_complexity(
    repo_root: Path,
    files_index: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Analyze SQL complexity across an entire repository.
    
    Returns aggregated complexity metrics for the repository.
    """
    analyzer = SQLComplexityAnalyzer()
    all_results = []
    
    complexity_distribution = {
        "simple": 0,
        "moderate": 0,
        "complex": 0,
        "very_complex": 0
    }
    
    total_joins = 0
    total_subqueries = 0
    total_ctes = 0
    total_window_functions = 0
    
    risk_flag_counts = {}
    
    for file_info in files_index:
        rel_path = file_info.get("path")
        if not rel_path:
            continue
        
        # Only analyze SQL-like files
        file_type = (file_info.get("detected_type") or "").lower()
        if file_type not in {"sql", "hql", "impala_sql"}:
            continue
        
        file_path = repo_root / rel_path
        try:
            sql_content = file_path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        
        # Analyze the file
        if sql_content.strip():
            result = analyzer.analyze_query(sql_content, str(rel_path), line_number=1)
            all_results.append(result.to_dict())
            
            # Update aggregated metrics
            complexity_distribution[result.complexity_level] += 1
            total_joins += result.join_analysis.total_joins
            total_subqueries += result.subquery_analysis.total_subqueries
            total_ctes += result.cte_analysis.total_ctes
            total_window_functions += result.window_function_analysis.total_window_functions
            
            # Count risk flags
            for flag in result.risk_flags:
                risk_flag_counts[flag] = risk_flag_counts.get(flag, 0) + 1
    
    # Calculate average complexity
    avg_complexity_score = (
        sum(r["total_complexity_score"] for r in all_results) / len(all_results)
        if all_results else 0
    )
    
    return {
        "queries_analyzed": len(all_results),
        "complexity_distribution": complexity_distribution,
        "average_complexity_score": round(avg_complexity_score, 2),
        "aggregated_metrics": {
            "total_joins": total_joins,
            "total_subqueries": total_subqueries,
            "total_ctes": total_ctes,
            "total_window_functions": total_window_functions
        },
        "risk_flag_summary": risk_flag_counts,
        "detailed_results": all_results,
        "top_10_most_complex": sorted(
            all_results,
            key=lambda x: x["total_complexity_score"],
            reverse=True
        )[:10]
    }
