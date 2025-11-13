import type { AnyObject } from "@cadenza.io/core";

export type SchemaType =
  | "varchar"
  | "text"
  | "int"
  | "bigint" // Added for BIGINT
  | "decimal"
  | "boolean"
  | "array"
  | "object"
  | "jsonb"
  | "uuid"
  | "timestamp"
  | "date"
  | "geo_point"
  | "bytea"
  | "any";

export const SCHEMA_TYPES = [
  "varchar",
  "text",
  "int",
  "bigint",
  "decimal",
  "boolean",
  "array",
  "object",
  "jsonb",
  "uuid",
  "timestamp",
  "date",
  "geo_point",
  "bytea",
  "any",
] as SchemaType[];

export type SchemaConstraints = {
  min?: number;
  max?: number;
  minLength?: number;
  maxLength?: number;
  pattern?: string;
  enum?: any[]; // TODO
  multipleOf?: number;
  format?: "email" | "url" | "date-time" | "uuid" | "custom";
  oneOf?: any[];
  appendOnly?: boolean;
  check?: string;
  precision?: number; // Added for decimal/bigint
  scale?: number; // Added for decimal
};

export interface FieldDefinition {
  type: SchemaType;
  primary?: boolean;
  index?: boolean;
  unique?: boolean;
  default?: any;
  required?: boolean;
  nullable?: boolean;
  encrypted?: boolean;
  constraints?: SchemaConstraints;
  references?: string;
  items?: FieldDefinition;
  description?: string;
  generated?: "uuid" | "timestamp" | "now" | "autoIncrement"; // Added autoIncrement
  onDelete?: "cascade" | "set null" | "no action" | "set default" | "restrict";
  onUpdate?: "cascade" | "set null" | "no action" | "set default"; // Added for constraint cascade
}

export type DbOperationType = "query" | "insert" | "update" | "delete";

export interface TableDefinition {
  fields: Record<string, FieldDefinition>;
  meta?: {
    description?: string;
    tags?: string[];
    shardKey?: string;
    partitionHint?: string;
    appendOnly?: boolean;
    encryptedFields?: string[];
  };
  indexes?: string[][];
  uniqueConstraints?: string[][];
  primaryKey?: string[];
  fullTextIndexes?: string[][];
  foreignKeys?: {
    tableName: string;
    fields: string[];
    referenceFields: string[];
    onDelete?: "cascade" | "set null" | "no action";
  }[];
  triggers?: Record<
    string,
    {
      when: "before" | "after";
      event: "insert" | "update" | "delete";
      function: string;
    }
  >;
  customSignals?: {
    triggers?: {
      [key in DbOperationType]?: (
        | string
        | { signal: string; condition?: (ctx: AnyObject) => boolean }
      )[];
    }; // Signals to observe before action
    emissions?: {
      [key in DbOperationType]?: (
        | string
        | { signal: string; condition?: (ctx: AnyObject) => boolean }
      )[];
    }; // Signals to emit after action
  };
  initialData?: { fields: string[]; data: any[][] };
}

export interface SchemaDefinition {
  version?: number;
  tables: Record<string, TableDefinition>;
  relations?: Record<
    string,
    {
      on: string;
      type?: "one-to-one" | "one-to-many" | "many-to-one" | "many-to-many";
    }
  >;
  meta?: {
    defaultEncoding?: "utf8" | "base64";
    autoIndex?: boolean;
    relationsVersion?: number;
    dropExisting?: boolean;
  };
}
