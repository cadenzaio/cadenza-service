import { describe, expect, it } from "vitest";
import type { SchemaDefinition } from "@cadenza.io/core";
import {
  getInsertDataSchemaFromTable,
  getQueryFilterSchemaFromTable,
  resolveTableOperationIntents,
  resolveTableQueryIntents,
} from "../src/database/DatabaseController";
import type { TableDefinition } from "../src/types/database";

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
});
