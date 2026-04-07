import type { AnyObject, SchemaDefinition } from "@cadenza.io/core";
import { DbOperationPayload } from "./queryData";

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
        | {
            signal: string;
            condition?: (ctx: AnyObject) => boolean;
            queryData?: DbOperationPayload;
          }
      )[];
    }; // Signals to observe before action
    emissions?: {
      [key in DbOperationType]?: (
        | string
        | { signal: string; condition?: (ctx: AnyObject) => boolean }
      )[];
    }; // Signals to emit after action
  };
  customIntents?: {
    query?: (
      | string
      | {
          intent: string;
          description?: string;
          input?: SchemaDefinition;
        }
    )[];
    insert?: (
      | string
      | {
          intent: string;
          description?: string;
          input?: SchemaDefinition;
        }
    )[];
    update?: (
      | string
      | {
          intent: string;
          description?: string;
          input?: SchemaDefinition;
        }
    )[];
    delete?: (
      | string
      | {
          intent: string;
          description?: string;
          input?: SchemaDefinition;
        }
    )[];
  };
  initialData?: { fields: string[]; data: any[][] };
}

export interface DatabaseMigrationPolicy {
  baselineOnEmpty?: boolean;
  adoptExistingVersion?: number | null;
  allowDestructive?: boolean;
  transactionalMode?: "per_migration" | "none";
}

export type DatabaseMigrationConstraintDefinition =
  | {
      kind: "primaryKey";
      fields: string[];
    }
  | {
      kind: "unique";
      fields: string[];
    }
  | {
      kind: "foreignKey";
      fields: string[];
      referenceTable: string;
      referenceFields: string[];
      onDelete?: "cascade" | "set null" | "no action";
    }
  | {
      kind: "check";
      expression: string;
    }
  | {
      kind: "sql";
      sql: string;
    };

export type DatabaseMigrationStep =
  | {
      kind: "createTable";
      table: string;
      definition: TableDefinition;
    }
  | {
      kind: "dropTable";
      table: string;
      ifExists?: boolean;
      cascade?: boolean;
    }
  | {
      kind: "addColumn";
      table: string;
      column: string;
      definition: FieldDefinition;
      ifNotExists?: boolean;
    }
  | {
      kind: "dropColumn";
      table: string;
      column: string;
      ifExists?: boolean;
      cascade?: boolean;
    }
  | {
      kind: "alterColumn";
      table: string;
      column: string;
      setType?: SchemaType;
      using?: string;
      setDefault?: any;
      dropDefault?: boolean;
      setNotNull?: boolean;
      dropNotNull?: boolean;
    }
  | {
      kind: "renameColumn";
      table: string;
      from: string;
      to: string;
    }
  | {
      kind: "renameTable";
      from: string;
      to: string;
    }
  | {
      kind: "addIndex";
      table: string;
      fields: string[];
      name?: string;
      unique?: boolean;
    }
  | {
      kind: "dropIndex";
      table?: string;
      fields?: string[];
      name?: string;
      ifExists?: boolean;
    }
  | {
      kind: "addConstraint";
      table: string;
      name: string;
      definition: DatabaseMigrationConstraintDefinition;
    }
  | {
      kind: "dropConstraint";
      table: string;
      name: string;
      ifExists?: boolean;
    }
  | {
      kind: "sql";
      sql: string | string[];
    };

export interface DatabaseMigrationDefinition {
  version: number;
  name: string;
  description?: string;
  steps: DatabaseMigrationStep[];
  transaction?: "inherit" | "required" | "none";
}

export interface DatabaseSchemaDefinition {
  version?: number;
  tables: Record<string, TableDefinition>;
  migrations?: DatabaseMigrationDefinition[];
  migrationPolicy?: DatabaseMigrationPolicy;
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
