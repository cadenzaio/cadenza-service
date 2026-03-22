import { describe, expect, it } from "vitest";
import type { SchemaDefinition } from "@cadenza.io/core";
import {
  serializeFieldDefaultForSql,
  getInsertDataSchemaFromTable,
  getQueryFilterSchemaFromTable,
  isTransientDatabaseError,
  mergeTriggerQueryData,
  resolveOperationPayload,
  resolveTableOperationIntents,
  resolveTableQueryIntents,
  serializeInitialDataValueForSql,
  serializeFieldValueForQuery,
} from "../src/database/DatabaseController";
import DatabaseController from "../src/database/DatabaseController";
import type { TableDefinition } from "../src/types/database";
import type { AnyObject } from "@cadenza.io/core";

describe("DatabaseController schema and intent helpers", () => {
  const table: TableDefinition = {
    fields: {
      uuid: {
        type: "uuid",
        primary: true,
      },
      score: {
        type: "int",
      },
    },
  };

  it("resolves default query intent", () => {
    const defaultSchema: SchemaDefinition = {
      type: "object",
      properties: {
        filter: { type: "object" },
      },
    };

    const { intents } = resolveTableQueryIntents(
      "ExampleService",
      "player",
      table,
      defaultSchema,
    );

    expect(intents).toHaveLength(1);
    expect(intents[0]).toMatchObject({
      name: "query-pg-example-service-player",
      input: defaultSchema,
    });
  });

  it("resolves custom query intents", () => {
    const defaultSchema: SchemaDefinition = {
      type: "object",
      properties: {
        filter: { type: "object" },
      },
    };
    const customSchema: SchemaDefinition = {
      type: "object",
      properties: {
        leaderboard: { type: "boolean" },
      },
    };

    const { intents } = resolveTableQueryIntents(
      "ExampleService",
      "player",
      {
        ...table,
        customIntents: {
          query: [
            "leaderboard-player-query",
            {
              intent: "search-player-query",
              input: customSchema,
            },
          ],
        },
      },
      defaultSchema,
    );

    expect(intents.map((intent) => intent.name)).toEqual([
      "query-pg-example-service-player",
      "leaderboard-player-query",
      "search-player-query",
    ]);
    expect(intents[2].input).toEqual(customSchema);
  });

  it("rejects duplicate or invalid custom query intent definitions", () => {
    const defaultSchema: SchemaDefinition = {
      type: "object",
      properties: {
        filter: { type: "object" },
      },
    };

    expect(() =>
      resolveTableQueryIntents(
        "ExampleService",
        "player",
        {
          ...table,
          customIntents: {
            query: ["leaderboard-player-query", "leaderboard-player-query"],
          },
        },
        defaultSchema,
      ),
    ).toThrow("Duplicate query intent 'leaderboard-player-query' on table 'player'");

    expect(() =>
      resolveTableQueryIntents(
        "ExampleService",
        "player",
        {
          ...table,
          customIntents: {
            query: ["invalid.intent.name"],
          },
        },
        defaultSchema,
      ),
    ).toThrow("Intent name cannot contain spaces, dots or backslashes");
  });

  it("builds actor-scoped default intents for all CRUD operations", () => {
    const defaultSchema: SchemaDefinition = {
      type: "object",
      properties: {
        filter: { type: "object" },
      },
    };

    expect(
      resolveTableOperationIntents(
        "ExampleTelemetryPostgresActor",
        "player",
        table,
        "query",
        defaultSchema,
      ).intents[0].name,
    ).toBe("query-pg-example-telemetry-postgres-actor-player");

    expect(
      resolveTableOperationIntents(
        "ExampleTelemetryPostgresActor",
        "player",
        table,
        "insert",
        defaultSchema,
      ).intents[0].name,
    ).toBe("insert-pg-example-telemetry-postgres-actor-player");

    expect(
      resolveTableOperationIntents(
        "ExampleTelemetryPostgresActor",
        "player",
        table,
        "update",
        defaultSchema,
      ).intents[0].name,
    ).toBe("update-pg-example-telemetry-postgres-actor-player");

    expect(
      resolveTableOperationIntents(
        "ExampleTelemetryPostgresActor",
        "player",
        table,
        "delete",
        defaultSchema,
      ).intents[0].name,
    ).toBe("delete-pg-example-telemetry-postgres-actor-player");
  });

  it("builds insert data schema as keyed variants", () => {
    const schema = getInsertDataSchemaFromTable(table, "player") as Record<
      string,
      any
    >;

    expect(Array.isArray(schema)).toBe(false);
    expect(schema.single?.type).toBe("object");
    expect(schema.batch?.type).toBe("array");
    expect(schema.single?.properties?.uuid?.value?.type).toBe("string");
    expect(schema.single?.properties?.uuid?.subOperation?.properties?.data)
      .toHaveProperty("single");
    expect(schema.single?.properties?.uuid?.subOperation?.properties?.data)
      .toHaveProperty("batch");
  });

  it("builds query filter schema as keyed variants", () => {
    const schema = getQueryFilterSchemaFromTable(table, "player");
    const fieldSchema = schema.properties?.uuid as Record<string, any>;

    expect(Array.isArray(fieldSchema)).toBe(false);
    expect(fieldSchema.value?.type).toBe("string");
    expect(fieldSchema.in?.type).toBe("array");
  });

  it("keeps explicit SQL jsonb literals unquoted in initial data", () => {
    expect(
      serializeInitialDataValueForSql(
        '\'{"minLength": 0, "maxLength": 255}\'::jsonb',
        {
          type: "jsonb",
        },
      ),
    ).toBe('\'{"minLength": 0, "maxLength": 255}\'::jsonb');
  });

  it("serializes plain jsonb seed values as jsonb literals", () => {
    expect(
      serializeInitialDataValueForSql(
        {
          schema: {},
        },
        {
          type: "jsonb",
        },
      ),
    ).toBe('\'{"schema":{}}\'::jsonb');
  });

  it("quotes plain string defaults and preserves explicit SQL expressions", () => {
    expect(serializeFieldDefaultForSql("eager", { type: "varchar" })).toBe("'eager'");
    expect(serializeFieldDefaultForSql("", { type: "varchar" })).toBe("''");
    expect(serializeFieldDefaultForSql("now()", { type: "timestamp" })).toBe("now()");
    expect(serializeFieldDefaultForSql("gen_random_uuid()", { type: "uuid" })).toBe(
      "gen_random_uuid()",
    );
    expect(serializeFieldDefaultForSql("'{}'", { type: "jsonb" })).toBe("'{}'");
    expect(serializeFieldDefaultForSql(null, { type: "jsonb" })).toBe("NULL");
  });

  it("serializes runtime jsonb values for parameterized queries", () => {
    expect(
      serializeFieldValueForQuery(["rest", "socket"], {
        type: "jsonb",
      }),
    ).toBe('["rest","socket"]');

    expect(
      serializeFieldValueForQuery(
        {
          runtimeState: "healthy",
        },
        {
          type: "jsonb",
        },
      ),
    ).toBe('{"runtimeState":"healthy"}');

    expect(
      serializeFieldValueForQuery("'[\"rest\",\"socket\"]'::jsonb", {
        type: "jsonb",
      }),
    ).toBe('["rest","socket"]');

    expect(
      serializeFieldValueForQuery("plain-text", {
        type: "jsonb",
      }),
    ).toBe('"plain-text"');
  });

  it("treats execution observability FK races as transient", () => {
    expect(
      isTransientDatabaseError(
        {
          code: "23503",
          constraint: "task_execution_map_task_execution_id_fkey",
          message:
            'insert or update on table "task_execution_map" violates foreign key constraint "task_execution_map_task_execution_id_fkey"',
        },
        "Insert task_execution_map",
      ),
    ).toBe(true);

    expect(
      isTransientDatabaseError(
        {
          code: "23503",
          table: "routine_execution",
          message:
            'insert or update on table "routine_execution" violates foreign key constraint "routine_execution_execution_trace_id_fkey"',
        },
        "Insert routine_execution",
      ),
    ).toBe(true);
  });

  it("does not treat unrelated FK violations as transient", () => {
    expect(
      isTransientDatabaseError(
        {
          code: "23503",
          constraint: "service_instance_transport_service_instance_id_fkey",
          message:
            'insert or update on table "service_instance_transport" violates foreign key constraint "service_instance_transport_service_instance_id_fkey"',
        },
        "Insert service_instance_transport",
      ),
    ).toBe(false);
  });

  it("merges trigger queryData without dropping the existing operation payload", () => {
    expect(
      mergeTriggerQueryData(
        {
          data: {
            name: "OrdersService",
          },
        } as AnyObject,
        {
          onConflict: {
            target: ["name"],
            action: {
              do: "nothing",
            },
          },
        } as AnyObject,
      ),
    ).toEqual(
      expect.objectContaining({
        data: {
          name: "OrdersService",
        },
        onConflict: {
          target: ["name"],
          action: {
            do: "nothing",
          },
        },
      }),
    );
  });

  it("resolves operation payloads by backfilling root fields into partial queryData", () => {
    expect(
      resolveOperationPayload({
        data: {
          name: "CadenzaDB",
        },
        queryData: {
          onConflict: {
            target: ["name"],
            action: {
              do: "nothing",
            },
          },
        },
      } as AnyObject),
    ).toEqual(
      expect.objectContaining({
        data: {
          name: "CadenzaDB",
        },
        onConflict: {
          target: ["name"],
          action: {
            do: "nothing",
          },
        },
      }),
    );
  });

  it("uses the default serializer when generating column DDL", () => {
    const controller = Object.create(DatabaseController.prototype) as DatabaseController;
    const fieldDefinitionToSql = (
      controller as unknown as {
        fieldDefinitionToSql: (fieldName: string, field: TableDefinition["fields"][string]) => string;
      }
    ).fieldDefinitionToSql;

    expect(
      fieldDefinitionToSql("loadPolicy", {
        type: "varchar",
        default: "eager",
      }),
    ).toBe("load_policy VARCHAR(255) DEFAULT 'eager'");

    expect(
      fieldDefinitionToSql("created", {
        type: "timestamp",
        default: "now()",
      }),
    ).toBe("created TIMESTAMP DEFAULT now()");
  });

  it("does not treat intent schema fragments as executable sub-operations", async () => {
    const controller = Object.create(DatabaseController.prototype) as DatabaseController;
    const resolveNestedData = (
      controller as unknown as {
        resolveNestedData: (
          registration: AnyObject,
          data: unknown,
          tableName: string,
        ) => Promise<unknown>;
      }
    ).resolveNestedData.bind(controller);

    const registration = {
      schema: {
        tables: {
          intent_registry: {
            fields: {
              name: { type: "varchar", primary: true },
              input: { type: "jsonb" },
            },
          },
        },
      },
    };

    const inputSchema = {
      type: "object",
      properties: {
        metric: {
          value: {
            type: "string",
          },
          subOperation: {
            type: "object",
            properties: {
              subOperation: {
                type: "string",
              },
              table: {
                type: "string",
              },
            },
          },
        },
      },
    };

    await expect(
      resolveNestedData(
        registration,
        {
          name: "query-pg-example-service-metric",
          input: inputSchema,
        },
        "intent_registry",
      ),
    ).resolves.toEqual({
      name: "query-pg-example-service-metric",
      input: inputSchema,
    });
  });

  it("builds idempotent constraint DDL without dropping existing constraints", () => {
    const controller = Object.create(DatabaseController.prototype) as DatabaseController;
    const buildSchemaDdlStatements = (
      controller as unknown as {
        buildSchemaDdlStatements: (
          schema: AnyObject,
          sortedTables: string[],
        ) => string[];
      }
    ).buildSchemaDdlStatements.bind(controller);

    const ddl = buildSchemaDdlStatements(
      {
        tables: {
          routine: {
            fields: {
              name: { type: "varchar", required: true },
              service_name: { type: "varchar", required: true },
              version: { type: "int", default: 1 },
            },
            primaryKey: ["name", "service_name", "version"],
            uniqueConstraints: [["name", "service_name"]],
          },
          task_to_routine_map: {
            fields: {
              routine_name: { type: "varchar", required: true },
              routine_version: { type: "int", default: 1 },
              service_name: { type: "varchar", required: true },
            },
            foreignKeys: [
              {
                tableName: "routine",
                fields: ["routine_name", "routine_version", "service_name"],
                referenceFields: ["name", "version", "service_name"],
              },
            ],
          },
        },
      },
      ["routine", "task_to_routine_map"],
    );

    expect(ddl.some((statement) => statement.includes("DROP CONSTRAINT"))).toBe(false);
    expect(ddl).toContain(
      "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pk_routine_name_service_name_version' AND conrelid = 'routine'::regclass) THEN ALTER TABLE routine ADD CONSTRAINT pk_routine_name_service_name_version PRIMARY KEY (name, service_name, version); END IF; END $$;",
    );
    expect(ddl).toContain(
      "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uq_routine_name_service_name' AND conrelid = 'routine'::regclass) THEN ALTER TABLE routine ADD CONSTRAINT uq_routine_name_service_name UNIQUE (name, service_name); END IF; END $$;",
    );
    expect(ddl).toContain(
      "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_task_to_routine_map_routine_name_routine_version_service_name' AND conrelid = 'task_to_routine_map'::regclass) THEN ALTER TABLE task_to_routine_map ADD CONSTRAINT fk_task_to_routine_map_routine_name_routine_version_service_name FOREIGN KEY (routine_name, routine_version, service_name) REFERENCES routine (name, version, service_name); END IF; END $$;",
    );
  });
});
