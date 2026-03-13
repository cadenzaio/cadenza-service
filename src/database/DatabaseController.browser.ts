export default class DatabaseController {
  private static _instance: DatabaseController;

  public static get instance(): DatabaseController {
    if (!this._instance) {
      this._instance = new DatabaseController();
    }

    return this._instance;
  }

  private unsupported(message: string): never {
    throw new Error(message);
  }

  createPostgresActor(): never {
    return this.unsupported(
      "PostgresActor creation is not supported in the browser runtime.",
    );
  }

  requestPostgresActorSetup(): never {
    return this.unsupported(
      "PostgresActor setup is not supported in the browser runtime.",
    );
  }
}
