import {
  DbOperationType,
  SchemaDefinition,
  SCHEMA_TYPES,
  FieldDefinition,
  TableDefinition,
} from "../types/database";
import Cadenza, { DatabaseOptions, ServerOptions } from "../Cadenza";
import { Pool, PoolClient } from "pg";
import { snakeCase } from "lodash-es";
import { AnyObject } from "@cadenza.io/core";
import {
  DbOperationPayload,
  JoinDefinition,
  SubOperation,
} from "../types/queryData";

export default class DatabaseController {
  private static _instance: DatabaseController;
  public static get instance(): DatabaseController {
    if (!this._instance) this._instance = new DatabaseController();
    return this._instance;
  }

  dbClient = new Pool({
    user: process.env.DATABASE_USER ?? "postgres",
    host: process.env.DATABASE_ADDRESS ?? "localhost",
    port: parseInt(process.env.DATABASE_PORT ?? "5432"),
    database: "postgres",
    password: process.env.DATABASE_PASSWORD ?? "03gibnEF",
  });

  reset() {
    this.dbClient.end();
  }

  constructor() {
    Cadenza.createMetaRoutine(
      "DatabaseServiceInit",
      [
        // Database health check
        // Create database role
        // Create schema version table
        Cadenza.createMetaTask(
          "Create database",
          async (ctx) => {
            const { databaseName } = ctx;
            try {
              if (
                !databaseName.split("").every((c: string) => /[a-z_]/.test(c))
              ) {
                throw new Error(
                  `Invalid database name ${databaseName}. Names must only contain lowercase alphanumeric characters and underscores`,
                );
              }
              await this.dbClient.query(`CREATE DATABASE ${databaseName}`);
              console.log(`Database ${databaseName} created`);
              // Update dbClient to use the new database
              this.dbClient = new Pool({
                user: process.env.DATABASE_USER ?? "postgres",
                host: process.env.DATABASE_ADDRESS ?? "localhost",
                port: parseInt(process.env.DATABASE_PORT ?? "5432"),
                database: databaseName,
                password: process.env.DATABASE_PASSWORD ?? "03gibnEF",
              });
              return true;
            } catch (error: any) {
              if (error.code === "42P04") {
                console.log("Database already exists");
                // Database already exists
                return true;
              }
              throw new Error(`Failed to create database: ${error.message}`);
            }
          },
          "Creates the target database if it doesn't exist",
        ).then(
          Cadenza.createMetaTask(
            "Validate schema",
            (ctx) => {
              const { schema } = ctx as {
                schema: SchemaDefinition;
                options: ServerOptions & DatabaseOptions;
              };
              if (!schema?.tables || typeof schema.tables !== "object") {
                throw new Error("Invalid schema: missing or invalid tables");
              }
              for (const [tableName, table] of Object.entries(schema.tables)) {
                if (!table.fields || typeof table.fields !== "object") {
                  console.log(tableName, "missing fields");
                  throw new Error(`Invalid table ${tableName}: missing fields`);
                }

                // Validate FieldDefinition constraints and customSignals
                for (const [fieldName, field] of Object.entries(table.fields)) {
                  if (!fieldName.split("").every((c) => /[a-z_]/.test(c))) {
                    console.log(tableName, "field not lowercase", fieldName);
                    throw new Error(
                      `Invalid field name ${fieldName} for ${tableName}. Field names must only contain lowercase alphanumeric characters and underscores`,
                    );
                  }
                  if (!Object.values(SCHEMA_TYPES).includes(field.type)) {
                    console.log(
                      tableName,
                      "field invalid type",
                      fieldName,
                      field.type,
                    );
                    throw new Error(
                      `Invalid type ${field.type} for ${tableName}.${fieldName}`,
                    );
                  }
                  if (
                    field.references &&
                    !field.references.match(/^[\w]+[(\w)]+$/)
                  ) {
                    console.log(
                      tableName,
                      "invalid reference",
                      fieldName,
                      field.references,
                    );
                    throw new Error(
                      `Invalid reference ${field.references} for ${tableName}.${fieldName}`,
                    );
                  }
                  if (table.customSignals) {
                    for (const op of ["query", "insert", "update", "delete"]) {
                      const triggers =
                        table.customSignals.triggers?.[op as DbOperationType];
                      const emissions =
                        table.customSignals.emissions?.[op as DbOperationType];
                      if (
                        triggers &&
                        !Array.isArray(triggers) &&
                        typeof triggers !== "object"
                      ) {
                        console.log(
                          tableName,
                          "invalid triggers",
                          op,
                          triggers,
                        );
                        throw new Error(
                          `Invalid triggers for ${tableName}.${op}`,
                        );
                      }
                      if (
                        emissions &&
                        !Array.isArray(emissions) &&
                        typeof emissions !== "object"
                      ) {
                        console.log(
                          tableName,
                          "invalid emissions",
                          op,
                          emissions,
                        );
                        throw new Error(
                          `Invalid emissions for ${tableName}.${op}`,
                        );
                      }
                    }
                  }
                }
              }
              console.log("SCHEMA VALIDATED");
              return true;
            },
            "Validates database schema structure and content",
          ).then(
            Cadenza.createMetaTask(
              "Sort tables by dependencies",
              this.sortTablesByReferences.bind(this),
              "Sorts tables by dependencies",
            ).then(
              Cadenza.createMetaTask(
                "Split schema into tables",
                this.splitTables.bind(this),
                "Generates DDL for database schema",
              ).then(
                Cadenza.createMetaTask(
                  "Generate tasks",
                  (ctx) => {
                    const { table, tableName, options } = ctx;

                    this.createDatabaseTask(
                      "query",
                      tableName,
                      table,
                      this.queryFunction.bind(this),
                      options,
                    );

                    this.createDatabaseTask(
                      "insert",
                      tableName,
                      table,
                      this.insertFunction.bind(this),
                      options,
                    );

                    this.createDatabaseTask(
                      "update",
                      tableName,
                      table,
                      this.updateFunction.bind(this),
                      options,
                    );

                    this.createDatabaseTask(
                      "delete",
                      tableName,
                      table,
                      this.deleteFunction.bind(this),
                      options,
                    );
                  },
                  "Generates auto-tasks for database schema",
                ),
                Cadenza.createMetaTask("Generate DDL from table", (ctx) => {
                  const { ddl, table, tableName, schema, options } = ctx;
                  const fieldDefs = Object.entries(table.fields)
                    .map((value) => {
                      const [fieldName, field] = value as [
                        string,
                        FieldDefinition,
                      ];
                      let def = `${fieldName} ${field.type.toUpperCase()}`;
                      if (field.type === "varchar")
                        def += `(${field.constraints?.maxLength ?? 255})`;
                      if (field.type === "decimal")
                        def += `(${field.constraints?.precision ?? 10},${field.constraints?.scale ?? 2})`;
                      if (field.primary) def += " PRIMARY KEY";
                      if (field.unique) def += " UNIQUE";
                      if (field.default !== undefined)
                        def += ` DEFAULT ${field.default === "" ? "''" : String(field.default)}`;
                      if (field.required && !field.nullable) def += " NOT NULL";
                      if (field.nullable) def += " NULL";
                      if (field.generated)
                        def += ` GENERATED ALWAYS AS ${field.generated.toUpperCase()} STORED`;
                      if (field.references)
                        // TODO "FOREIGN KEY (foo_id, created_on) REFERENCES foo (id, created_on)", for composite primary keys
                        def += ` REFERENCES ${field.references} ON DELETE ${field.onDelete || "NO_ACTION"}`;
                      if (field.encrypted) def += " ENCRYPTED"; // Pseudo, handle via app-side

                      if (field.constraints?.check) {
                        def += ` CHECK (${field.constraints.check})`;
                      }
                      return def;
                    })
                    .join(", ");

                  let sql = `CREATE TABLE IF NOT EXISTS ${tableName} (${fieldDefs})`;
                  // if (table.meta?.appendOnly) { // TODO Add prevent_context_modification() function
                  //   sql += `;\nCREATE TRIGGER prevent_modification BEFORE UPDATE OR DELETE ON ${tableName} FOR EACH STATEMENT EXECUTE FUNCTION prevent_context_modification();`;
                  // }

                  ddl.push(sql);

                  return { ddl, table, tableName, schema, options };
                }).then(
                  Cadenza.createMetaTask("Generate index DDL", (ctx) => {
                    const { ddl, table, tableName, schema, options } = ctx;
                    if (table.indexes) {
                      table.indexes.forEach((fields: string[]) => {
                        ddl.push(
                          `CREATE INDEX IF NOT EXISTS idx_${tableName}_${fields.join("_")} ON ${tableName} (${fields.join(", ")});`,
                        );
                      });
                    }

                    return { ddl, table, tableName, schema, options };
                  }).then(
                    Cadenza.createMetaTask(
                      "Generate unique index DDL",
                      (ctx) => {
                        const { ddl, table, tableName, schema, options } = ctx;
                        if (table.uniqueConstraints) {
                          table.uniqueConstraints.forEach(
                            (fields: string[]) => {
                              ddl.push(
                                `ALTER TABLE ${tableName} DROP CONSTRAINT IF EXISTS unique_${tableName}_${fields.join("_")};`,
                                `ALTER TABLE ${tableName} ADD CONSTRAINT unique_${tableName}_${fields.join("_")} UNIQUE (${fields.join(", ")});`,
                              );
                            },
                          );
                        }

                        return { ddl, table, tableName, schema, options };
                      },
                    ).then(
                      Cadenza.createMetaTask(
                        "Generate foreign key DDL",
                        (ctx) => {
                          const { ddl, table, tableName, schema, options } =
                            ctx;
                          if (table.foreignKeys) {
                            for (const foreignKey of table.foreignKeys as {
                              tableName: string;
                              fields: string[];
                              referenceFields: string[];
                            }[]) {
                              const foreignKeyName = `fk_${tableName}_${foreignKey.fields.join("_")}`;
                              ddl.push(
                                `ALTER TABLE ${tableName} DROP CONSTRAINT IF EXISTS ${foreignKeyName};`,
                                `ALTER TABLE ${tableName} ADD CONSTRAINT ${foreignKeyName} FOREIGN KEY (${foreignKey.fields.join(
                                  ", ",
                                )}) REFERENCES ${foreignKey.tableName} (${foreignKey.referenceFields.join(
                                  ", ",
                                )});`,
                              );
                            }
                          }
                          return { ddl, table, tableName, schema, options };
                        },
                      ).then(
                        Cadenza.createMetaTask(
                          "Generate trigger DDL",
                          (ctx) => {
                            const { ddl, table, tableName, schema, options } =
                              ctx;
                            if (table.triggers) {
                              for (const [
                                triggerName,
                                trigger,
                              ] of Object.entries(table.triggers) as [
                                string,
                                any,
                              ][]) {
                                ddl.push(
                                  `CREATE TRIGGER ${triggerName} ${trigger.when} ${trigger.event} ON ${tableName} FOR EACH STATEMENT EXECUTE FUNCTION ${trigger.function};`,
                                );
                              }
                            }
                            return { ddl, table, tableName, schema, options };
                          },
                        ).then(
                          Cadenza.createMetaTask(
                            "Generate initial data DDL",
                            (ctx) => {
                              const { ddl, table, tableName, schema, options } =
                                ctx;
                              if (table.initialData) {
                                ddl.push(
                                  `INSERT INTO ${tableName} (${table.initialData.fields.join(", ")}) VALUES ${table.initialData.data
                                    .map(
                                      (row: any[]) =>
                                        `(${row
                                          .map((value) =>
                                            value === undefined
                                              ? "NULL"
                                              : value.charAt(0) === "'"
                                                ? value
                                                : `'${value}'`,
                                          )
                                          .join(", ")})`, // TODO: handle non string data
                                    )
                                    .join(", ")} ON CONFLICT DO NOTHING;`,
                                );
                              }

                              return { ddl, table, tableName, schema, options };
                            },
                          ).then(
                            Cadenza.createUniqueMetaTask("Join DDL", (ctx) => {
                              const { joinedContexts } = ctx;
                              const ddl: string[] = [];
                              for (const joinedContext of joinedContexts) {
                                ddl.push(...joinedContext.ddl);
                              }
                              ddl.flat();
                              return {
                                ddl,
                                schema: joinedContexts[0].schema,
                                options: joinedContexts[0].options,
                              };
                            }).then(
                              Cadenza.createMetaTask(
                                "meta.applyDatabaseChanges",
                                async (ctx) => {
                                  const { ddl } = ctx;
                                  if (ddl && ddl.length > 0) {
                                    try {
                                      for (const sql of ddl) {
                                        console.log("Applying SQL", sql);
                                        await this.dbClient.query(sql);
                                      }
                                    } catch (error: any) {
                                      console.error(
                                        "Error applying DDL",
                                        error,
                                      );
                                      throw error;
                                    }
                                  }
                                  console.log("DDL applied");
                                  return ctx;
                                },
                                "Applies generated DDL to the database",
                              ).emits("meta.database.setup_done"),
                            ),
                          ),
                        ),
                      ),
                    ),
                  ),
                ),
              ),
            ),
          ),
        ),
      ],
      "Initializes the database service with schema parsing and task/signal generation",
    ).doOn("meta.database_init_requested");
  }

  private async getClient(): Promise<PoolClient> {
    const client = (await this.dbClient.connect()) as unknown as any;
    const query = client.query;
    const release = client.release;
    // set a timeout of 5 seconds, after which we will log this client's last query
    const timeout = setTimeout(() => {
      console.error("A client has been checked out for more than 5 seconds!");
      console.error(
        `The last executed query on this client was: ${client.lastQuery}`,
      );
    }, 5000);
    // monkey patch the query method to keep track of the last query executed
    client.query = (...args: any[]) => {
      client.lastQuery = args;
      return query.apply(client, args);
    };
    client.release = () => {
      // clear our timeout
      clearTimeout(timeout);
      // set the methods back to their old un-monkey-patched version
      client.query = query;
      client.release = release;
      return release.apply(client);
    };
    return client;
  }

  private async waitForDatabase(
    transaction: (
      client: any,
      context: AnyObject,
    ) => Promise<AnyObject | boolean>,
    client: any,
    context: AnyObject,
  ) {
    for (let i = 0; i < 10; i++) {
      try {
        return await transaction(client, context);
      } catch (err: unknown) {
        if (err && (err as Error).message.includes("does not exist")) {
          console.log(`Waiting for database to be ready...`);
          await new Promise((res) => setTimeout(res, 1000));
        } else {
          console.error("Database query errored: ", err, context);
          return { rows: [] };
        }
      }
    }
    throw new Error(`Timeout waiting for database to be ready`);
  }

  sortTablesByReferences(ctx: AnyObject): AnyObject {
    // Build dependency graph: map of table -> set of dependent tables
    const schema: SchemaDefinition = ctx.schema;
    const graph: Map<string, Set<string>> = new Map();
    const allTables = Object.keys(schema.tables);

    // Initialize graph with all tables
    allTables.forEach((table) => graph.set(table, new Set()));

    // Populate dependencies, skipping self-references for cycle detection
    for (const [tableName, table] of Object.entries(schema.tables)) {
      for (const field of Object.values(table.fields)) {
        if (field.references) {
          const [refTable] = field.references.split("("); // Extract referenced table
          if (refTable !== tableName && allTables.includes(refTable)) {
            graph.get(refTable)?.add(tableName); // refTable depends on tableName
          }
        }
      }
    }

    // Topological sort using DFS with cycle detection
    const visited: Set<string> = new Set();
    const tempMark: Set<string> = new Set(); // For cycle detection
    const sorted: string[] = [];
    let hasCycles = false;

    function visit(table: string) {
      if (tempMark.has(table)) {
        hasCycles = true; // Mark cycle but continue
        return;
      }
      if (visited.has(table)) return;

      tempMark.add(table);
      for (const dep of graph.get(table) || []) {
        visit(dep);
      }
      tempMark.delete(table);
      visited.add(table);
      sorted.push(table);
    }

    // Visit each unvisited table
    for (const table of allTables) {
      if (!visited.has(table)) {
        visit(table);
      }
    }

    // Handle unvisited tables (e.g., no dependencies)
    for (const table of allTables) {
      if (!visited.has(table)) {
        sorted.push(table);
      }
    }

    sorted.reverse();

    console.log("sorted tables", sorted, "has cycles", hasCycles);

    return { ...ctx, sortedTables: sorted, hasCycles };
  }

  async *splitTables(ctx: any) {
    const { sortedTables, schema, options = {} } = ctx;
    for (const tableName of sortedTables) {
      const table = schema.tables[tableName];
      yield { ddl: [], table, tableName, schema, options };
    }
  }

  async queryFunction(
    tableName: string,
    context: DbOperationPayload,
  ): Promise<any> {
    const {
      filter = {},
      fields = [],
      joins = {},
      sort = {},
      limit,
      offset,
    } = context;

    // Build base query
    let sql = `SELECT ${fields.length ? fields.join(", ") : "*"} FROM ${tableName}`;
    const params: any[] = [];
    const conditions: string[] = [];

    // Handle filter
    if (Object.keys(filter).length > 0) {
      conditions.push(this.buildWhereClause(filter, params));
    }

    // Handle joins
    if (Object.keys(joins).length > 0) {
      sql += " " + this.buildJoinClause(joins);
    }

    // Handle sort
    if (Object.keys(sort).length > 0) {
      sql +=
        " ORDER BY " +
        Object.entries(sort)
          .map(([field, direction]) => `${field} ${direction}`)
          .join(", ");
    }

    // Handle limit and offset
    if (limit !== undefined) sql += ` LIMIT $${params.length + 1}`;
    params.push(limit);
    if (offset !== undefined) sql += ` OFFSET $${params.length + 1}`;
    params.push(offset);

    try {
      const result = await this.dbClient.query(sql, params);
      return {
        [`${tableName}s`]: result.rows,
        rowCount: result.rowCount,
        __success: true,
        ...context,
      };
    } catch (error: any) {
      return {
        errored: true,
        __error: `Query failed: ${error.message}`,
        __errors: { query: error.message },
      };
    }
  }

  async insertFunction(
    tableName: string,
    context: DbOperationPayload,
  ): Promise<any> {
    const { data, transaction = true, fields = [], onConflict } = context;

    if (!data || (Array.isArray(data) && data.length === 0)) {
      return { errored: true, __error: "No data provided for insert" };
    }

    const client = transaction ? await this.getClient() : this.dbClient;
    try {
      if (transaction) await client.query("BEGIN");

      const resolvedData = await this.resolveNestedData(data, tableName);
      const isBatch = Array.isArray(resolvedData);
      const rows = isBatch ? resolvedData : [resolvedData];

      const sql = `INSERT INTO ${tableName} (${Object.keys(rows[0]).join(", ")}) VALUES `;
      const values = rows
        .map(
          (row) =>
            `(${Object.values(row)
              .map((value: any, i) => {
                if (typeof value === "object" && value?.__effect) {
                  if (value.__effect === "increment") {
                    return `${Object.keys(row)[i]} + 1`;
                  }
                  if (value.__effect === "decrement") {
                    return `${Object.keys(row)[i]} - 1`;
                  }
                  if (value.__effect === "set") {
                    return `${Object.keys(row)[i]} = ${value.__value}`; // TODO: placeholder, not working
                  }
                }
                return `$${i + 1}`;
              })
              .join(", ")})`,
        )
        .join(", ");
      const params = rows.flatMap((row) => Object.values(row));

      let onConflictSql = "";
      if (onConflict) {
        const { target, action } = onConflict;
        onConflictSql += ` ON CONFLICT (${target.join(", ")})`;
        if (action.do === "update") {
          if (!action.set || Object.keys(action.set).length === 0) {
            throw new Error("Update action requires 'set' fields");
          }
          const setClauses = Object.entries(action.set)
            .map(
              ([field, value]) =>
                `${field} = ${value === "excluded" ? "excluded." + field : `$${params.length + 1}`}`,
            )
            .join(", ");
          params.push(
            ...Object.values(action.set).filter(
              (v) => typeof v !== "string" || !v.startsWith("excluded."),
            ),
          );
          onConflictSql += ` DO UPDATE SET ${setClauses}`;
          if (action.where) onConflictSql += ` WHERE ${action.where}`;
        } else {
          onConflictSql += ` DO NOTHING`;
        }
      }

      const result = await client.query(
        `${sql} ${values}${onConflictSql} RETURNING ${fields.length ? fields.join(", ") : "*"}`,
        params,
      );
      if (transaction) await client.query("COMMIT");
      return {
        [`${tableName}${isBatch ? "s" : ""}`]: isBatch
          ? result.rows
          : result.rows[0],
        rowCount: result.rowCount,
        __inserted: true,
      };
    } catch (error: any) {
      if (transaction) await client.query("ROLLBACK");
      return {
        errored: true,
        __error: `Insert failed: ${error.message}`,
        __errors: { insert: error.message },
      };
    } finally {
      if (transaction && client) {
        // @ts-ignore
        client.release();
      }
    }
  }

  async updateFunction(
    tableName: string,
    context: DbOperationPayload,
  ): Promise<any> {
    const { data, filter = {}, transaction = true } = context;

    if (!data || Object.keys(data).length === 0) {
      return { errored: true, __error: "No data provided for update" };
    }

    const client = transaction ? await this.getClient() : this.dbClient;
    try {
      if (transaction) await client.query("BEGIN");

      const resolvedData = await this.resolveNestedData(data, tableName);
      const setClause = Object.entries(resolvedData)
        .map(([key, value]) => `${key} = $${params.length + 1}`)
        .join(", ");
      const params = Object.values(resolvedData);
      const whereClause = this.buildWhereClause(filter, params);

      const sql = `UPDATE ${tableName} SET ${setClause} ${whereClause} RETURNING *`;
      const result = await client.query(sql, params);
      if (transaction) await client.query("COMMIT");
      return {
        [`${tableName}`]: result.rows[0],
        __updated: true,
      };
    } catch (error: any) {
      if (transaction) await client.query("ROLLBACK");
      return {
        errored: true,
        __error: `Update failed: ${error.message}`,
        __errors: { update: error.message },
      };
    } finally {
      if (transaction && client) {
        // @ts-ignore
        client.release();
      }
    }
  }

  async deleteFunction(
    tableName: string,
    context: DbOperationPayload,
  ): Promise<any> {
    const { filter = {}, transaction = true } = context;

    if (Object.keys(filter).length === 0) {
      return { errored: true, __error: "No filter provided for delete" };
    }

    const client = transaction ? await this.getClient() : this.dbClient;
    try {
      if (transaction) await client.query("BEGIN");

      const params: any[] = [];
      const whereClause = this.buildWhereClause(filter, params);
      const sql = `DELETE FROM ${tableName} ${whereClause} RETURNING *`;
      const result = await client.query(sql, params);
      if (transaction) await client.query("COMMIT");
      return {
        [`${tableName}`]: result.rows[0],
        __deleted: true,
      };
    } catch (error: any) {
      if (transaction) await client.query("ROLLBACK");
      return {
        errored: true,
        __error: `Delete failed: ${error.message}`,
        __errors: { delete: error.message },
      };
    } finally {
      if (transaction && client) {
        // @ts-ignore
        client.release();
      }
    }
  }

  buildWhereClause(filter: AnyObject, params: any[]): string {
    const conditions = [];
    for (const [key, value] of Object.entries(filter)) {
      if (value !== undefined) {
        conditions.push(`${snakeCase(key)} = $${params.length + 1}`);
        params.push(value);
      }
    }
    return conditions.length ? `WHERE ${conditions.join(" AND ")}` : "";
  }

  buildJoinClause(joins: Record<string, JoinDefinition>): string {
    let joinSql = "";
    for (const [table, join] of Object.entries(joins)) {
      joinSql += ` LEFT JOIN ${snakeCase(table)} ${join.alias} ON ${join.on}`;
      if (join.joins) joinSql += " " + this.buildJoinClause(join.joins);
    }
    return joinSql;
  }

  async resolveNestedData(data: any, tableName: string): Promise<any> {
    if (Array.isArray(data))
      return Promise.all(data.map((d) => this.resolveNestedData(d, tableName)));
    if (typeof data !== "object" || data === null) return data;

    const resolved = { ...data };
    for (const [key, value] of Object.entries(data)) {
      if (
        typeof value === "object" &&
        value !== null &&
        "subOperation" in value
      ) {
        const subOp = value as SubOperation;
        const subResult = await this.executeSubOperation(subOp);
        resolved[snakeCase(key)] =
          subResult[subOp.return || "full"] ?? subResult;
      } else if (
        typeof value === "string" &&
        ["increment", "decrement", "set"].includes(value)
      ) {
        resolved[snakeCase(key)] = { __effect: value }; // Placeholder for effect handling (DB-side or app-side)
      } else if (typeof value === "object") {
        resolved[snakeCase(key)] = await this.resolveNestedData(
          value,
          tableName,
        );
      }
    }
    return resolved;
  }

  async executeSubOperation(op: SubOperation): Promise<any> {
    const client = await this.getClient();
    try {
      await client.query("BEGIN");
      let result;
      if (op.subOperation === "insert") {
        const resolvedData = await this.resolveNestedData(op.data, op.table);
        const sql = `INSERT INTO ${op.table} (${Object.keys(resolvedData).join(", ")}) VALUES (${Object.values(
          resolvedData,
        )
          .map((_, i) => `$${i + 1}`)
          .join(
            ", ",
          )}) ON CONFLICT DO NOTHING RETURNING ${op.return === "id" ? "id" : "*"}`;
        result =
          ((await client.query(sql, Object.values(resolvedData))).rows[0] ??
          op.return === "id")
            ? resolvedData.id
              ? { id: resolvedData.id }
              : undefined
            : undefined;
      } else if (op.subOperation === "query") {
        const params: any[] = [];
        const whereClause = this.buildWhereClause(op.filter || {}, params);
        const sql = `SELECT ${op.fields?.join(", ") || "*"} FROM ${op.table} ${whereClause} LIMIT 1`;
        result = (await client.query(sql, params)).rows[0];
      }
      await client.query("COMMIT");
      return result || {};
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  createDatabaseTask(
    op: DbOperationType,
    tableName: string,
    table: TableDefinition,
    queryFunction: (tableName: string, context: AnyObject) => Promise<any>,
    options: ServerOptions,
  ) {
    const defaultSignal = `${tableName}.${op}`;
    Cadenza.createTask(
      `db${op.charAt(0).toUpperCase() + op.slice(1)}${tableName.charAt(0).toUpperCase() + tableName.slice(1)}`,
      async (context, emit) => {
        const triggerConditions: any | undefined =
          table.customSignals?.triggers?.query?.filter(
            (trigger: any) => trigger.condition,
          );
        for (const triggerCondition of triggerConditions ?? []) {
          if (
            triggerCondition.condition &&
            !triggerCondition.condition(context)
          ) {
            return {
              failed: true,
              error: `Condition for signal trigger failed: ${triggerCondition.signal}`,
            };
          }
        }

        context = await queryFunction(tableName, context.querydata ?? context);

        for (const signal of table.customSignals?.emissions?.[op] ??
          ([] as any[])) {
          if (signal.condition && !signal.condition(context)) {
            continue;
          }
          emit(signal.signal ?? signal, context);
        }

        delete context.queryData;
        delete context.data;
        delete context.filter;
        delete context.fields;
        delete context.joins;
        delete context.sort;
        delete context.limit;
        delete context.offset;

        return context;
      },
      `Auto-generated ${op} task for ${tableName}`,
      {
        isMeta: options.isMeta,
        concurrency: 50,
        validateInputContext: false, // TODO
        getTagCallback: (
          context?: AnyObject, // TODO more granular tags
        ) =>
          context?.__metadata?.__executionTraceId ??
          context?.__executionTraceId ??
          "default",
        inputSchema: {
          // TODO
          type: "object",
          properties: {
            filter: {
              type: "object",
            },
          },
          required: ["filter"],
        },
      },
    )
      .doOn(
        ...(table.customSignals?.triggers?.[op]?.map((signal: any) => {
          return typeof signal === "string" ? signal : signal.signal;
        }) ?? []),
      )
      .emits(defaultSignal);
    console.log("Created database task", op, tableName);
  }
}
