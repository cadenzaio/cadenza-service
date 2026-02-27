import { describe, expect, it } from "vitest";
import type { SchemaDefinition } from "@cadenza.io/core";
import {
  getInsertDataSchemaFromTable,
  getQueryFilterSchemaFromTable,
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

    const { intents, warnings } = resolveTableQueryIntents(
      "ExampleService",
      "player",
      table,
      defaultSchema,
    );

    expect(warnings).toHaveLength(0);
    expect(intents).toHaveLength(1);
    expect(intents[0]).toMatchObject({
      name: "query-ExampleService-player",
      input: defaultSchema,
    });
  });

  it("resolves custom query intents and skips invalid/duplicate definitions", () => {
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

    const { intents, warnings } = resolveTableQueryIntents(
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
            "leaderboard-player-query",
            "",
            "invalid.intent.name",
          ],
        },
      },
      defaultSchema,
    );

    expect(intents.map((intent) => intent.name)).toEqual([
      "query-ExampleService-player",
      "leaderboard-player-query",
      "search-player-query",
    ]);
    expect(intents[2].input).toEqual(customSchema);
    expect(warnings).toHaveLength(3);
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
