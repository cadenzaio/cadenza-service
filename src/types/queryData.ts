// Define supported operation types
import type { AnyObject } from "@cadenza.io/core";

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
  return?: string; // "uuid" | "single" | "array" | "full"; What to return (e.g., 'uuid' for insert ID or 'name' for name column)
  // No joins in sub-ops to limit depth/complexity
}

export interface OnConflictAction {
  do: "nothing" | "update"; // Action on conflict
  set?: Record<string, any>; // Fields to update (for DO UPDATE)
  where?: string; // Optional WHERE clause for conditional update (e.g., "excluded.created > context.created")
}

export type OpEffect = "increment" | "decrement" | "set";

export type ValueOrSubOp = any | SubOperation | OpEffect; // Field value or nested op
export type ValueOrList = any | any[];

// Define query data payload
export interface DbOperationPayload {
  data?: Record<string, ValueOrSubOp> | Record<string, ValueOrSubOp>[]; // Nested sub-ops in data
  filter?: Record<string, ValueOrList>; // For query/update/delete
  fields?: string[];
  joins?: Record<string, JoinDefinition>; // For query
  sort?: Record<string, SortDirection>;
  limit?: number; // For query
  offset?: number; // For query
  transaction?: boolean; // Wrap in transaction (default true for nested)
  batch?: boolean; // For array data
  onConflict?: {
    target: string[]; // Target columns
    action: OnConflictAction;
  };
  // Future expansions: groupBy, having, batchSize, etc.
}
