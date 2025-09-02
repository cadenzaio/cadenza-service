// Define supported operation types
import { AnyObject } from "@cadenza.io/core";

export type DbOperationType = "query" | "insert" | "update" | "delete";

// Define sort direction
export type SortDirection = "asc" | "desc";

// Define join definition (recursive for nested joins)
export interface JoinDefinition {
  on: string; // Join condition (e.g., "service_name = task.service_name")
  fields: string[]; // Fields to select from joined table
  returnAs?: "array" | "object"; // Return as array (for 1-to-many) or object (default: 'object')
  filter?: AnyObject; // Optional sub-filter for joined table
  alias?: string; // Optional alias for the joined field in result
  joins?: Record<string, JoinDefinition>; // Nested joins (recursive)
}

export type SubOperationType = "insert" | "query"; // Limit to safe ops (no update/delete in nests for safety)

export interface SubOperation {
  subOperation: SubOperationType;
  table: string;
  data?: AnyObject | AnyObject[]; // For insert
  filter?: AnyObject; // For query
  fields?: string[]; // For query
  return?: "id" | "single" | "array" | "full"; // What to return (e.g., 'id' for insert ID)
  // No __joins in sub-ops to limit depth/complexity
}

export type OpEffect = "increment" | "decrement" | "set";

export type ValueOrSubOp = any | SubOperation | OpEffect; // Field value or nested op

// Define query data payload
export interface DbOperationPayload {
  data?: Record<string, ValueOrSubOp> | Record<string, ValueOrSubOp>[]; // Nested sub-ops in data
  filter?: AnyObject; // For query/update/delete
  fields?: string[];
  joins?: Record<string, JoinDefinition>; // For query
  sort?: Record<string, SortDirection>;
  limit?: number; // For query
  offset?: number; // For query
  transaction?: boolean; // Wrap in transaction (default true for nested)
  batch?: boolean; // For array __data
  // Future expansions: __groupBy, __having, __batchSize, etc.
}
