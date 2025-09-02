import Cadenza from "../../Cadenza";

export default class TaskController {
  private static _instance: TaskController;
  public static get instance(): TaskController {
    if (!this._instance) this._instance = new TaskController();
    return this._instance;
  }

  constructor() {
    Cadenza.createMetaTask("Add data to task creation", (ctx) => {
      return {
        ...ctx.__task,
        serviceName: Cadenza.serviceRegistry.serviceName,
      };
    })
      .doOn("meta.task.created")
      .emitsAfter("meta.task_controller.task_created");
  }
}
