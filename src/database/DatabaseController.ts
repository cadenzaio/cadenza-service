import {
  DbOperationType,
  SchemaDefinition,
  SCHEMA_TYPES,
  FieldDefinition,
  TableDefinition,
} from "../types/database";
import Cadenza, { DatabaseOptions, ServerOptions } from "../Cadenza";
import { Pool, PoolClient } from "pg";
import { camelCase, snakeCase } from "lodash-es";
import type { AnyObject } from "@cadenza.io/core";
import {
  DbOperationPayload,
  JoinDefinition,
  SubOperation,
} from "../types/queryData";
import { sleep } from "../utils/promise";

/**
 * DatabaseController is a singleton class that manages database connections,
 * schema validation, and database initialization tasks. It provides mechanisms
 * to create new databases, validate schemas, and manage the database lifecycle.
 */
export default class DatabaseController {
  private static _instance: DatabaseController;
  public static get instance(): DatabaseController {
    if (!this._instance) this._instance = new DatabaseController();
    return this._instance;
  }

  databaseName: string = "";

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

  /**
   * Constructor for initializing the `DatabaseService` class.
   *
   * This constructor method initializes a sequence of meta tasks to perform the following database-related operations:
   *
   * 1. **Database Creation**: Creates a new database with the specified name if it doesn't already exist.
   *    Validates the database name to ensure it conforms to the required format.
   * 2. **Database Schema Validation**: Validates the structure and constraints of the schema definition provided.
   * 3. **Table Dependency Management**: Sorts tables within the schema by their dependencies to ensure proper creation order.
   * 4. **Schema Definition Processing**:
   *    - Converts schema definitions into Data Definition Language (DDL) based on table and field specifications.
   *    - Handles constraints, relationships, and field attributes such as uniqueness, primary keys, nullable fields, etc.
   * 5. **Index and Primary Key Definition**: Generates SQL for indices and primary keys based on the schema configuration.
   *
   * These tasks are encapsulated within a meta routine to provide a structured and procedural approach to database initialization and schema management.
   */
  constructor() {
    Cadenza.createMetaRoutine(
      "DatabaseServiceInit",
      [
        // TODO: Database health check
        // TODO: Create database role
        // TODO: Create schema version table
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

              this.databaseName = databaseName;
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
                  "Generate DDL from table",
                  async (ctx) => {
                    const {
                      ddl,
                      table,
                      tableName,
                      schema,
                      options,
                      sortedTables,
                    } = ctx;
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
                        if (field.required && !field.nullable)
                          def += " NOT NULL";
                        if (field.nullable) def += " NULL";
                        if (field.generated)
                          def += ` GENERATED ALWAYS AS ${field.generated.toUpperCase()} STORED`;
                        if (field.references)
                          def += ` REFERENCES ${field.references} ON DELETE ${field.onDelete || "DO NOTHING"}`;
                        if (field.encrypted) def += " ENCRYPTED"; // Pseudo, handle via app-side

                        if (field.constraints?.check) {
                          def += ` CHECK (${field.constraints.check})`;
                        }
                        return def;
                      })
                      .join(", ");

                    if (schema.meta?.dropExisting) {
                      const result = await this.dbClient.query(
                        `DELETE FROM ${tableName};`,
                      );
                      console.log("DROP TABLE", tableName, result);
                    }

                    const sql = `CREATE TABLE IF NOT EXISTS ${tableName} (${fieldDefs})`;
                    // if (table.meta?.appendOnly) { // TODO Add prevent_context_modification() function
                    //   sql += `;\nCREATE TRIGGER prevent_modification BEFORE UPDATE OR DELETE ON ${tableName} FOR EACH STATEMENT EXECUTE FUNCTION prevent_context_modification();`;
                    // }

                    ddl.push(sql);

                    return {
                      ddl,
                      table,
                      tableName,
                      schema,
                      options,
                      sortedTables,
                    };
                  },
                ).then(
                  Cadenza.createMetaTask("Generate index DDL", (ctx) => {
                    const {
                      ddl,
                      table,
                      tableName,
                      schema,
                      options,
                      sortedTables,
                    } = ctx;
                    if (table.indexes) {
                      table.indexes.forEach((fields: string[]) => {
                        ddl.push(
                          `CREATE INDEX IF NOT EXISTS idx_${tableName}_${fields.join("_")} ON ${tableName} (${fields.join(", ")});`,
                        );
                      });
                    }

                    return {
                      ddl,
                      table,
                      tableName,
                      schema,
                      options,
                      sortedTables,
                    };
                  }).then(
                    Cadenza.createMetaTask(
                      "Generate primary key ddl",
                      (ctx) => {
                        const {
                          ddl,
                          table,
                          tableName,
                          schema,
                          options,
                          sortedTables,
                        } = ctx;
                        if (table.primaryKey) {
                          ddl.push(
                            `ALTER TABLE ${tableName} DROP CONSTRAINT IF EXISTS unique_${tableName}_${table.primaryKey.join("_")};`, // TODO: should be cascade?
                            `ALTER TABLE ${tableName} ADD CONSTRAINT unique_${tableName}_${table.primaryKey.join("_")} PRIMARY KEY (${table.primaryKey.join(", ")});`,
                          );
                        }

                        return {
                          ddl,
                          table,
                          tableName,
                          schema,
                          options,
                          sortedTables,
                        };
                      },
                    ).then(
                      Cadenza.createMetaTask(
                        "Generate unique index DDL",
                        (ctx) => {
                          const {
                            ddl,
                            table,
                            tableName,
                            schema,
                            options,
                            sortedTables,
                          } = ctx;
                          if (table.uniqueConstraints) {
                            table.uniqueConstraints.forEach(
                              (fields: string[]) => {
                                ddl.push(
                                  `ALTER TABLE ${tableName} DROP CONSTRAINT IF EXISTS unique_${tableName}_${fields.join("_")};`, // TODO: should be cascade?
                                  `ALTER TABLE ${tableName} ADD CONSTRAINT unique_${tableName}_${fields.join("_")} UNIQUE (${fields.join(", ")});`,
                                );
                              },
                            );
                          }

                          return {
                            ddl,
                            table,
                            tableName,
                            schema,
                            options,
                            sortedTables,
                          };
                        },
                      ).then(
                        Cadenza.createMetaTask(
                          "Generate foreign key DDL",
                          (ctx) => {
                            const {
                              ddl,
                              table,
                              tableName,
                              schema,
                              options,
                              sortedTables,
                            } = ctx;
                            if (table.foreignKeys) {
                              for (const foreignKey of table.foreignKeys as {
                                tableName: string;
                                fields: string[];
                                referenceFields: string[];
                              }[]) {
                                const foreignKeyName = `fk_${tableName}_${foreignKey.fields.join("_")}`;
                                ddl.push(
                                  `ALTER TABLE ${tableName} DROP CONSTRAINT IF EXISTS ${foreignKeyName};`, // TODO: should be cascade?
                                  `ALTER TABLE ${tableName} ADD CONSTRAINT ${foreignKeyName} FOREIGN KEY (${foreignKey.fields.join(
                                    ", ",
                                  )}) REFERENCES ${foreignKey.tableName} (${foreignKey.referenceFields.join(
                                    ", ",
                                  )});`,
                                );
                              }
                            }
                            return {
                              ddl,
                              table,
                              tableName,
                              schema,
                              options,
                              sortedTables,
                            };
                          },
                        ).then(
                          Cadenza.createMetaTask(
                            "Generate trigger DDL",
                            (ctx) => {
                              const {
                                ddl,
                                table,
                                tableName,
                                schema,
                                options,
                                sortedTables,
                              } = ctx;
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
                              return {
                                ddl,
                                table,
                                tableName,
                                schema,
                                options,
                                sortedTables,
                              };
                            },
                          ).then(
                            Cadenza.createMetaTask(
                              "Generate initial data DDL",
                              (ctx) => {
                                const {
                                  ddl,
                                  table,
                                  tableName,
                                  schema,
                                  options,
                                  sortedTables,
                                } = ctx;
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

                                return {
                                  ddl,
                                  table,
                                  tableName,
                                  schema,
                                  options,
                                  sortedTables,
                                };
                              },
                            ).then(
                              Cadenza.createUniqueMetaTask(
                                "Join DDL",
                                (ctx) => {
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
                                    table: joinedContexts[0].table,
                                    tableName: joinedContexts[0].tableName,
                                    sortedTables:
                                      joinedContexts[0].sortedTables,
                                  };
                                },
                              ).then(
                                Cadenza.createMetaTask(
                                  "Apply Database Changes",
                                  async (ctx) => {
                                    const { ddl } = ctx;
                                    if (ddl && ddl.length > 0) {
                                      for (const sql of ddl) {
                                        try {
                                          console.log("Applying SQL", sql);
                                          await this.dbClient.query(sql);
                                        } catch (error: any) {
                                          console.error(
                                            "Error applying DDL",
                                            error,
                                          );
                                        }
                                      }
                                    }
                                    return true;
                                  },
                                  "Applies generated DDL to the database",
                                ).then(
                                  Cadenza.createMetaTask(
                                    "Split schema into tables for task creation",
                                    this.splitTables.bind(this),
                                    "Splits schema into tables for task creation",
                                  ).then(
                                    Cadenza.createMetaTask(
                                      "Generate tasks",
                                      (ctx) => {
                                        const { table, tableName, options } =
                                          ctx;

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

                                        return true;
                                      },
                                      "Generates auto-tasks for database schema",
                                    ).then(
                                      Cadenza.createUniqueMetaTask(
                                        "Join table tasks",
                                        (ctx, emit) => {
                                          emit("meta.database.setup_done", {});
                                          return true;
                                        },
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
                ),
              ),
            ),
          ),
        ),
      ],
      "Initializes the database service with schema parsing and task/signal generation",
    ).doOn("meta.database_init_requested");
  }

  /**
   * Asynchronously retrieves a database client from the connection pool with additional logging and timeout capabilities.
   * The method modifies the client instance by adding timeout tracking and logging functionality to ensure
   * the client is not held for an extended period and track the last executed query for debugging purposes.
   *
   * @return {Promise<PoolClient>} A promise resolving to a database client from the pool with enhanced behavior for query tracking and timeout handling.
   */
  private async getClient(): Promise<PoolClient> {
    const client = (await this.dbClient.connect()) as unknown as any;
    const query = client.query;
    const release = client.release;
    // set a timeout of 5 seconds, after which we will log this client's last query
    const timeout = setTimeout(() => {
      Cadenza.log(
        "CRITICAL: A database client has been checked out for more than 5 seconds!",
        {
          clientId: client.uuid,
          query: client.lastQuery,
          databaseName: this.databaseName,
        },
        "critical",
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
          Cadenza.log("Waiting for database to be ready...");
          await new Promise((res) => setTimeout(res, 1000));
        } else {
          Cadenza.log(
            "Database query errored",
            { error: err, context },
            "warning",
          );
          return { rows: [] };
        }
      }
    }
    throw new Error(`Timeout waiting for database to be ready`);
  }

  /**
   * Sorts database tables based on their reference dependencies using a topological sort.
   *
   * Tables are reordered such that dependent tables appear later in the list
   * to ensure a dependency hierarchy. If cycles are detected in the dependency graph,
   * they will be noted but the process will not stop. Unreferenced tables are included at the end.
   *
   * @param {Object} ctx - The context object containing the database schema definition and table metadata.
   *        ctx.schema {Object} - The schema definition object.
   *        ctx.schema.tables {Object} - A mapping of table names to table definitions.
   *        Each table definition may contain `fields` (with `references` info)
   *        and `foreignKeys` indicating cross-table relationships.
   *
   * @return {Object} - The modified context object with an additional property:
   *         sortedTables {string[]} - An array of table names sorted in dependency order.
   *         hasCycles {boolean} - Indicates if the dependency graph contains cycles.
   */
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

      if (table.foreignKeys) {
        for (const foreignKey of table.foreignKeys) {
          const refTable = foreignKey.tableName;
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
    return { ...ctx, sortedTables: sorted, hasCycles };
  }

  /**
   * Asynchronously creates an iterator that splits the provided tables from the schema.
   *
   * @param {Object} ctx - The context object containing the necessary data.
   * @param {string[]} ctx.sortedTables - An array of table names sorted in a specific order.
   * @param {Object} ctx.schema - The schema object that includes table definitions.
   * @param {Object} [ctx.options={}] - Optional configuration options for processing tables.
   *
   * @return {AsyncGenerator} An asynchronous generator that yields objects containing the table definition, metadata, and other context details.
   */
  async *splitTables(ctx: any) {
    const { sortedTables, schema, options = {} } = ctx;
    for (const tableName of sortedTables) {
      const table = schema.tables[tableName];
      yield { ddl: [], table, tableName, schema, options, sortedTables };
    }
  }

  /**
   * Converts the keys of objects in an array to camelCase format.
   *
   * @param {Array<any>} rows - An array of objects whose keys should be converted to camelCase.
   * @return {Array<any>} A new array of objects with their keys converted to camelCase.
   */
  toCamelCase(rows: any[]) {
    return rows.map((row: any) => {
      const camelCasedRow: any = {};
      for (const [key, value] of Object.entries(row)) {
        camelCasedRow[camelCase(key)] = value;
      }
      return camelCasedRow;
    });
  }

  /**
   * Executes a query against a specified database table with given parameters.
   *
   * @param {string} tableName - The name of the database table to query.
   * @param {DbOperationPayload} context - An object containing query parameters such as filters, fields, joins, sort, limit, and offset.
   * @return {Promise<any>} A promise that resolves with the query result, including rows, row count, and metadata, or an error object if the query fails.
   */
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

    // Handle filter
    if (Object.keys(filter).length > 0) {
      sql += " " + this.buildWhereClause(filter, params);
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
    if (limit !== undefined) {
      sql += ` LIMIT $${params.length + 1}`;
      params.push(limit);
    }
    if (offset !== undefined) {
      sql += ` OFFSET $${params.length + 1}`;
      params.push(offset);
    }

    try {
      const result = await this.dbClient.query(sql, params);

      const rows = this.toCamelCase(result.rows);

      return {
        [`${camelCase(tableName)}s`]: rows,
        rowCount: result.rowCount,
        __success: true,
        ...context,
      };
    } catch (error: any) {
      return {
        ...context,
        errored: true,
        __error: `Query failed: ${error.message}`,
        __success: false,
      };
    }
  }

  /**
   * Inserts data into the specified database table with optional conflict handling.
   *
   * @param {string} tableName - The name of the target database table.
   * @param {DbOperationPayload} context - The context containing data to insert, transaction settings, field mappings, conflict resolution options, and other configurations.
   *   - `data` (object | array): The data to be inserted into the database.
   *   - `transaction` (boolean): Specifies whether the operation should use a transaction. Defaults to true.
   *   - `fields` (array): The fields to return in the result after insertion.
   *   - `onConflict` (object): Options for handling conflicts on insert.
   *     - `target` (array): Columns to determine conflicts.
   *     - `action` (object): Specifies the action to take on conflict, such as updating specified fields.
   *   - `awaitExists` (object): Specifies foreign key references to wait for to ensure existence before insertion.
   *
   * @return {Promise<any>} A promise resolving to the result of the database insert operation, including the inserted rows, the row count, and metadata indicating success or error.
   */
  async insertFunction(
    tableName: string,
    context: DbOperationPayload,
  ): Promise<any> {
    const {
      data,
      transaction = true,
      fields = [],
      onConflict,
      awaitExists,
    } = context;

    if (!data || (Array.isArray(data) && data.length === 0)) {
      return { errored: true, __error: "No data provided for insert" };
    }

    const client = transaction ? await this.getClient() : this.dbClient;
    try {
      if (awaitExists) {
        for (const fk of Object.keys(awaitExists)) {
          const value = (data as any)[fk];

          if (value === undefined || value === null) continue;

          const { table, column } = awaitExists[fk];
          let exists = false;
          let retries = 0;
          const maxRetries = 20;
          while (!exists && retries < maxRetries) {
            const result = await client.query(
              `SELECT EXISTS(SELECT 1 from ${table} WHERE ${column} = ${typeof value === "string" ? `'${value}'` : value}) AS "exists"`,
            );
            exists = result.rows[0].exists;
            if (exists) break;
            retries++;
            await sleep(100);
          }
        }
      }
      if (transaction) await client.query("BEGIN");

      const resolvedData = await this.resolveNestedData(data, tableName);
      const isBatch = Array.isArray(resolvedData);
      const rows = isBatch ? resolvedData : [resolvedData];

      const sql = `INSERT INTO ${tableName} (${Object.keys(rows[0]).map(snakeCase).join(", ")}) VALUES `;
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
      const resultRows = this.toCamelCase(result.rows);

      return {
        [`${camelCase(tableName)}${isBatch ? "s" : ""}`]: isBatch
          ? resultRows
          : resultRows[0],
        rowCount: result.rowCount,
        __success: true,
      };
    } catch (error: any) {
      if (transaction) await client.query("ROLLBACK");
      return {
        ...context,
        errored: true,
        __error: `Insert failed: ${error.message}`,
        __success: false,
      };
    } finally {
      if (transaction && client) {
        // @ts-ignore
        client.release();
      }
    }
  }

  /**
   * Updates a database table with the provided data and filter conditions.
   *
   * @param {string} tableName - The name of the database table to update.
   * @param {DbOperationPayload} context - The payload for the update operation, which includes:
   *        - data: The data to update in the table.
   *        - filter: The conditions to identify the rows to update (default is an empty object).
   *        - transaction: Whether the operation should run within a database transaction (default is true).
   * @return {Promise<any>} Returns a Promise resolving to an object that includes:
   *         - The updated data if the update is successful.
   *         - In case of error:
   *           - Error details.
   *           - The SQL query and parameters if applicable.
   *         - A flag indicating if the update succeeded or failed.
   */
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
      const params = Object.values(resolvedData);

      let offset = 0;
      const setClause = Object.entries(Object.keys(resolvedData))
        .map(([i, key]) => {
          const value = resolvedData[key];
          const offsetIndex = parseInt(i) - offset;
          if (value.__effect === "increment") {
            params.splice(offsetIndex, 1);
            offset++;
            return `${snakeCase(key)} = ${snakeCase(key)} + 1`;
          }
          if (value.__effect === "decrement") {
            params.splice(offsetIndex, 1);
            offset++;
            return `${snakeCase(key)} = ${snakeCase(key)} - 1`;
          }
          if (value.__effect === "set") {
            params.splice(offsetIndex, 1);
            offset++;
            return `${snakeCase(key)} = ${value.__value}`; // TODO: placeholder, not working
          }
          return `${snakeCase(key)} = $${offsetIndex + 1}`;
        })
        .join(", ");

      const whereClause = this.buildWhereClause(filter, params);

      const sql = `UPDATE ${tableName} SET ${setClause} ${whereClause} RETURNING *;`;
      const result = await client.query(sql, params);
      if (transaction) await client.query("COMMIT");
      const rows = this.toCamelCase(result.rows);

      if (rows.length === 0) {
        return {
          sql,
          params,
          __success: false,
        };
      }

      return {
        [`${camelCase(tableName)}`]: rows[0],
        __success: true,
      };
    } catch (error: any) {
      if (transaction) await client.query("ROLLBACK");
      return {
        ...context,
        errored: true,
        __error: `Update failed: ${error.message}`,
        __success: false,
      };
    } finally {
      if (transaction && client) {
        // @ts-ignore
        client.release();
      }
    }
  }

  /**
   * Deletes a record from the specified database table based on the given filter criteria.
   *
   * @param {string} tableName - The name of the database table from which records should be deleted.
   * @param {DbOperationPayload} context - The context for the operation, including filter conditions and transaction settings.
   * @param {Object} context.filter - The filter criteria to identify the records to delete.
   * @param {boolean} [context.transaction=true] - Indicates if the operation should be executed within a transaction.
   * @return {Promise<any>} A promise that resolves to an object containing information about the deleted record
   * or an error object if the delete operation fails.
   */
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
      const rows = this.toCamelCase(result.rows);
      return {
        [`${camelCase(tableName)}`]: rows[0],
        __success: true,
      };
    } catch (error: any) {
      if (transaction) await client.query("ROLLBACK");
      return {
        errored: true,
        __error: `Delete failed: ${error.message}`,
        __errors: { delete: error.message },
        __success: false,
      };
    } finally {
      if (transaction && client) {
        // @ts-ignore
        client.release();
      }
    }
  }

  /**
   * Constructs a SQL WHERE clause based on the provided filter object.
   * Builds parameterized queries to prevent SQL injection, appending parameters
   * to the provided params array and utilizing placeholders.
   *
   * @param {Object} filter - An object representing the filtering conditions with
   *                          keys as column names and values as their corresponding
   *                          desired values. Values can also be arrays for `IN` queries.
   * @param {any[]} params - An array for storing parameterized values, which will be
   *                         populated with the filter values for the constructed SQL clause.
   * @return {string} The constructed SQL WHERE clause as a string. If no conditions
   *                  are provided, an empty string is returned.
   */
  buildWhereClause(filter: AnyObject, params: any[]): string {
    const conditions = [];
    for (const [key, value] of Object.entries(filter)) {
      if (value !== undefined) {
        if (Array.isArray(value)) {
          conditions.push(
            `${snakeCase(key)} IN (${value
              .map((v) => {
                const val = `$${params.length + 1}`;
                params.push(v);
                return val;
              })
              .join(", ")})`,
          );
        } else {
          conditions.push(`${snakeCase(key)} = $${params.length + 1}`);
          params.push(value);
        }
      }
    }
    return conditions.length ? `WHERE ${conditions.join(" AND ")}` : "";
  }

  /**
   * Constructs a SQL join clause from a given set of join definitions.
   *
   * @param {Record<string, JoinDefinition>} joins - An object where keys are table names
   *                                                  and values are definitions of join conditions.
   * @return {string} The constructed SQL join clause as a string.
   */
  buildJoinClause(joins: Record<string, JoinDefinition>): string {
    let joinSql = "";
    for (const [table, join] of Object.entries(joins)) {
      joinSql += ` LEFT JOIN ${snakeCase(table)} ${join.alias} ON ${join.on}`;
      if (join.joins) joinSql += " " + this.buildJoinClause(join.joins);
    }
    return joinSql;
  }

  /**
   * Recursively resolves nested data structure by processing special operations and transforming the data accordingly.
   * Handles specific object structures with sub-operations, strings with specific commands, and other nested objects.
   *
   * @param {any} data The initial data to be resolved, which can be an object, array, or primitive value.
   * @param {string} tableName The name of the table associated with the data, used contextually for operation resolution.
   * @return {Promise<any>} A promise that resolves to the fully processed data structure with all nested elements resolved.
   */
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
        resolved[key] = subResult[subOp.return || "full"] ?? subResult;
      } else if (
        typeof value === "string" &&
        ["increment", "decrement", "set"].includes(value)
      ) {
        resolved[key] = { __effect: value }; // Placeholder for effect handling (DB-side or app-side)
      } else if (typeof value === "object") {
        resolved[key] = await this.resolveNestedData(value, tableName);
      }
    }
    return resolved;
  }

  /**
   * Executes a sub-operation against the database, such as an insert or query operation.
   *
   * @param {SubOperation} op - The operation to be executed. Contains details such as the type of sub-operation
   * (e.g., "insert" or "query"), the target table, data to be inserted, filters for querying, fields to be retrieved, etc.
   * @return {Promise<any>} A promise that resolves with the result of the operation.
   * For "insert", the result will include the inserted row or a partial response for uuid conflicts.
   * For "query", the result will include the first row that matches the query condition. If no result is found,
   * resolves with an empty object.
   * @throws Throws an error if the operation fails. Rolls back the transaction in case of an error.
   */
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
          )}) ON CONFLICT DO NOTHING RETURNING ${op.return === "uuid" ? "uuid" : "*"}`;
        result = await client.query(sql, Object.values(resolvedData));
        result = result.rows[0];
        if (!result && op.return === "uuid") {
          result = resolvedData.uuid ? { uuid: resolvedData.uuid } : undefined;
        }
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

  /**
   * Creates a database task configured for specific operations such as query, insert, update, or delete on a given table.
   *
   * @param {DbOperationType} op - The type of database operation to perform (e.g., "query", "insert", "update", "delete").
   * @param {string} tableName - The name of the table on which the operation will be performed.
   * @param {TableDefinition} table - The table definition that includes configurations such as custom signal triggers and emissions.
   * @param {function(string, AnyObject): Promise<any>} queryFunction - The function to execute the database operation. It takes the table name and a context object as arguments and returns a promise.
   * @param {ServerOptions} options - The options for configuring the server context and metadata behavior.
   * @return {void} This function does not return a value, but it registers a database task for the specified operation.
   */
  createDatabaseTask(
    op: DbOperationType,
    tableName: string,
    table: TableDefinition,
    queryFunction: (tableName: string, context: AnyObject) => Promise<any>,
    options: ServerOptions,
  ) {
    const opAction =
      op === "query"
        ? "queried"
        : op === "insert"
          ? "inserted"
          : op === "update"
            ? "updated"
            : op === "delete"
              ? "deleted"
              : "";

    const defaultSignal = `${options.isMeta ? "meta." : ""}${tableName}.${opAction}`;

    const tableNameFormatted = tableName
      .split("_")
      .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
      .join("");

    const taskName = `db${op.charAt(0).toUpperCase() + op.slice(1)}${tableNameFormatted}`;

    Cadenza.createThrottledTask(
      taskName,
      async (context: AnyObject, emit: any) => {
        for (const action of Object.keys(table.customSignals?.triggers ?? {})) {
          const triggerConditions: any | undefined = // @ts-ignore
            table.customSignals?.triggers?.[action].filter(
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

          const triggerQueryData: any | undefined = // @ts-ignore
            table.customSignals?.triggers?.[action].filter(
              (trigger: any) => trigger.queryData,
            );
          for (const queryData of triggerQueryData ?? []) {
            if (context.queryData) {
              context.queryData = {
                ...context.queryData,
                ...queryData,
              };
            } else {
              context = {
                ...context,
                ...queryData,
              };
            }
          }
        }

        try {
          context = await queryFunction(
            tableName,
            context.queryData ?? context,
          );
        } catch (e) {
          Cadenza.log(
            "Database task errored.",
            { taskName, error: e },
            "error",
          );
          throw e;
        }

        if (!context.errored) {
          for (const signal of table.customSignals?.emissions?.[op] ??
            ([] as any[])) {
            if (signal.condition && !signal.condition(context)) {
              continue;
            }
            emit(signal.signal ?? signal, context);
          }
        }

        if (tableName !== "system_log") {
          Cadenza.log(
            `EXECUTED ${taskName}`,
            context.errored
              ? JSON.stringify({
                  data: context.data,
                  queryData: context.queryData,
                  filter: context.filter,
                  fields: context.fields,
                  joins: context.joins,
                  sort: context.sort,
                  limit: context.limit,
                  offset: context.offset,
                  error: context.__error,
                })
              : {},
            context.errored ? "error" : context.__success ? "info" : "warning",
          );
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
      (context?: AnyObject) =>
        context?.__metadata?.__executionTraceId ??
        context?.__executionTraceId ??
        "default",
      `Auto-generated ${op} task for ${tableName}`,
      {
        isMeta: options.isMeta,
        isSubMeta: options.isMeta,
        validateInputContext: false, // TODO
        inputSchema: {
          // TODO
          type: "object",
          properties: {
            filter: {
              type: "object",
            },
          },
        },
      },
    )
      .doOn(
        ...(table.customSignals?.triggers?.[op]?.map((signal: any) => {
          return typeof signal === "string" ? signal : signal.signal;
        }) ?? []),
      )
      .emits(defaultSignal);
  }
}
