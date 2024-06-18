import DataAccessor from "./dataAccess/dataAccessor";
import { GetDataAccessorOptions, GetModelOptions, IDatabaseAccessor, IDatabaseConfig, IQueryBuilder, IRpdDataAccessor, RpdApplicationConfig, RpdDataModel, RpdServerEventTypes, RapidServerConfig, RpdDataModelProperty, CreateEntityOptions, UpdateEntityByIdOptions, EntityWatchHandlerContext, EntityWatcherType, RpdEntityCreateEventPayload } from "./types";

import QueryBuilder from "./queryBuilder/queryBuilder";
import PluginManager from "./core/pluginManager";
import EventManager from "./core/eventManager";
import { ActionHandler, ActionHandlerContext, IPluginActionHandler } from "./core/actionHandler";
import { IRpdServer, RapidPlugin } from "./core/server";
import { buildRoutes } from "./core/routesBuilder";
import { Next, RouteContext } from "./core/routeContext";
import { RapidRequest } from "./core/request";
import bootstrapApplicationConfig from "./bootstrapApplicationConfig";
import EntityManager from "./dataAccess/entityManager";
import { bind, cloneDeep, find, forEach, merge, omit } from "lodash";
import { Logger } from "./facilities/log/LogFacility";
import { FacilityFactory } from "./core/facility";

export interface InitServerOptions {
  logger: Logger;
  databaseAccessor: IDatabaseAccessor;
  databaseConfig: IDatabaseConfig;
  serverConfig: RapidServerConfig;
  applicationConfig?: RpdApplicationConfig;
  facilityFactories?: FacilityFactory[];
  plugins?: RapidPlugin[];
  entityWatchers?: EntityWatcherType[];
}

export class RapidServer implements IRpdServer {
  #logger: Logger;
  #facilityFactories: Map<string, FacilityFactory>;
  #pluginManager: PluginManager;
  #plugins: RapidPlugin[];
  #eventManager: EventManager<RpdServerEventTypes>;
  #middlewares: any[];
  #bootstrapApplicationConfig: RpdApplicationConfig;
  #applicationConfig: RpdApplicationConfig;
  #actionHandlersMapByCode: Map<string, ActionHandler>;
  #databaseAccessor: IDatabaseAccessor;
  #cachedDataAccessors: Map<string, DataAccessor>;

  #entityBeforeCreateEventEmitters: EventManager<Record<string, [EntityWatchHandlerContext<any>]>>;
  #entityCreateEventEmitters: EventManager<Record<string, [EntityWatchHandlerContext<any>]>>;
  #entityBeforeUpdateEventEmitters: EventManager<Record<string, [EntityWatchHandlerContext<any>]>>;
  #entityUpdateEventEmitters: EventManager<Record<string, [EntityWatchHandlerContext<any>]>>;
  #entityBeforeDeleteEventEmitters: EventManager<Record<string, [EntityWatchHandlerContext<any>]>>;
  #entityDeleteEventEmitters: EventManager<Record<string, [EntityWatchHandlerContext<any>]>>;
  #entityAddRelationsEventEmitters: EventManager<Record<string, [EntityWatchHandlerContext<any>]>>;
  #entityRemoveRelationsEventEmitters: EventManager<Record<string, [EntityWatchHandlerContext<any>]>>;
  #entityBeforeResponseEventEmitters: EventManager<Record<string, [EntityWatchHandlerContext<any>]>>;
  #entityWatchers: EntityWatcherType[];
  #appEntityWatchers: EntityWatcherType[];

  #cachedEntityManager: Map<string, EntityManager>;
  #services: Map<string, any>;
  queryBuilder: IQueryBuilder;
  config: RapidServerConfig;
  databaseConfig: IDatabaseConfig;
  #buildedRoutes: (ctx: any, next: any) => any;


  constructor(options: InitServerOptions) {
    this.#logger = options.logger;

    this.#facilityFactories = new Map();
    if (options.facilityFactories) {
      forEach(options.facilityFactories, (factory) => {
        this.registerFacilityFactory(factory);
      });
    }

    this.#pluginManager = new PluginManager(this);
    this.#eventManager = new EventManager();
    this.#middlewares = [];
    this.#bootstrapApplicationConfig = options.applicationConfig || bootstrapApplicationConfig;

    this.#applicationConfig = {} as RpdApplicationConfig;
    this.#actionHandlersMapByCode = new Map();
    this.#databaseAccessor = options.databaseAccessor;
    this.#cachedDataAccessors = new Map();
    this.#cachedEntityManager = new Map();

    this.#entityBeforeCreateEventEmitters = new EventManager();
    this.#entityCreateEventEmitters = new EventManager();
    this.#entityBeforeUpdateEventEmitters = new EventManager();
    this.#entityUpdateEventEmitters = new EventManager();
    this.#entityBeforeDeleteEventEmitters = new EventManager();
    this.#entityDeleteEventEmitters = new EventManager();
    this.#entityAddRelationsEventEmitters = new EventManager();
    this.#entityRemoveRelationsEventEmitters = new EventManager();
    this.#entityBeforeResponseEventEmitters = new EventManager();

    this.registerEventHandler("entity.beforeCreate", this.#handleEntityEvent.bind(this, "entity.beforeCreate"));
    this.registerEventHandler("entity.create", this.#handleEntityEvent.bind(this, "entity.create"));
    this.registerEventHandler("entity.beforeUpdate", this.#handleEntityEvent.bind(this, "entity.beforeUpdate"));
    this.registerEventHandler("entity.update", this.#handleEntityEvent.bind(this, "entity.update"));
    this.registerEventHandler("entity.beforeDelete", this.#handleEntityEvent.bind(this, "entity.beforeDelete"));
    this.registerEventHandler("entity.delete", this.#handleEntityEvent.bind(this, "entity.delete"));
    this.registerEventHandler("entity.addRelations", this.#handleEntityEvent.bind(this, "entity.addRelations"));
    this.registerEventHandler("entity.removeRelations", this.#handleEntityEvent.bind(this, "entity.removeRelations"));
    this.registerEventHandler("entity.beforeResponse", this.#handleEntityEvent.bind(this, "entity.beforeResponse"));

    this.#entityWatchers = [];
    this.#appEntityWatchers = options.entityWatchers || [];

    this.#services = new Map();

    this.queryBuilder = new QueryBuilder({
      dbDefaultSchema: options.databaseConfig.dbDefaultSchema,
    });
    this.databaseConfig = options.databaseConfig;
    this.config = options.serverConfig;

    this.#plugins = options.plugins || [];
  }

  getLogger(): Logger {
    return this.#logger;
  }

  getApplicationConfig() {
    return this.#applicationConfig;
  }

  appendApplicationConfig(config: Partial<RpdApplicationConfig>) {
    const { models, routes } = config;
    if (models) {
      for (const model of models) {
        const originalModel = find(this.#applicationConfig.models, (item) => item.singularCode == model.singularCode);
        if (originalModel) {
          merge(originalModel, omit(model, ["id", "maintainedBy", "namespace", "singularCode", "pluralCode", "schema", "tableName", "properties", "extensions"]));
          originalModel.name = model.name;
          const originalProperties = originalModel.properties;
          for (const property of model.properties) {
            const originalProperty = find(originalProperties, (item) => item.code == property.code);
            if (originalProperty) {
              originalProperty.name = property.name;
            } else {
              originalProperties.push(property);
            }
          }
        } else {
          this.#applicationConfig.models.push(model);
        }
      }
    }

    if (routes) {
      for (const route of routes) {
        const originalRoute = find(this.#applicationConfig.routes, (item) => item.code == route.code);
        if (originalRoute) {
          originalRoute.name = route.name;
          originalRoute.actions = route.actions;
        } else {
          this.#applicationConfig.routes.push(route);
        }
      }
    }
  }

  appendModelProperties(modelSingularCode: string, properties: RpdDataModelProperty[]) {
    const originalModel = find(this.#applicationConfig.models, (item) => item.singularCode == modelSingularCode);
    if (!originalModel) {
      throw new Error(`Cannot append model properties. Model '${modelSingularCode}' was not found.`);
    }

    const originalProperties = originalModel.properties;
    for (const property of properties) {
      const originalProperty = find(originalProperties, (item) => item.code == property.code);
      if (originalProperty) {
        originalProperty.name = property.name;
      } else {
        originalProperties.push(property);
      }
    }
  }

  registerActionHandler(plugin: RapidPlugin, options: IPluginActionHandler) {
    const handler = bind(options.handler, null, plugin);
    this.#actionHandlersMapByCode.set(options.code, handler);
  }

  getActionHandlerByCode(code: string) {
    return this.#actionHandlersMapByCode.get(code);
  }

  registerMiddleware(middleware: any) {
    this.#middlewares.push(middleware);
  }

  getDataAccessor<T = any>(options: GetDataAccessorOptions): IRpdDataAccessor<T> {
    const { namespace, singularCode } = options;

    let dataAccessor = this.#cachedDataAccessors.get(singularCode);
    if (dataAccessor) {
      return dataAccessor;
    }

    const model = this.getModel(options);
    if (!model) {
      throw new Error(`Data model ${namespace}.${singularCode} not found.`);
    }

    dataAccessor = new DataAccessor<T>(this, this.#databaseAccessor, {
      model,
      queryBuilder: this.queryBuilder as QueryBuilder,
    });
    this.#cachedDataAccessors.set(singularCode, dataAccessor);
    return dataAccessor;
  }

  getModel(options: GetModelOptions): RpdDataModel | undefined {
    if (options.namespace) {
      return this.#applicationConfig?.models.find((e) => e.namespace === options.namespace && e.singularCode === options.singularCode);
    }

    return this.#applicationConfig?.models.find((e) => e.singularCode === options.singularCode);
  }

  getEntityManager<TEntity = any>(singularCode: string): EntityManager<TEntity> {
    let entityManager = this.#cachedEntityManager.get(singularCode);
    if (entityManager) {
      return entityManager;
    }

    const dataAccessor = this.getDataAccessor({ singularCode });
    entityManager = new EntityManager(this, dataAccessor);
    this.#cachedEntityManager.set(singularCode, entityManager);
    return entityManager;
  }

  registerEventHandler<K extends keyof RpdServerEventTypes>(eventName: K, listener: (...args: RpdServerEventTypes[K]) => void) {
    this.#eventManager.on(eventName, listener);
  }

  registerEntityWatcher(entityWatcher: EntityWatcherType) {
    this.#entityWatchers.push(entityWatcher);
  }

  async emitEvent<K extends keyof RpdServerEventTypes>(eventName: K, payload: RpdServerEventTypes[K][1], sender?: RapidPlugin) {
    this.#logger.debug(`Emitting '${eventName}' event.`, { eventName, payload });
    await this.#eventManager.emit<K>(eventName, sender, payload as any);

    // TODO: should move this logic into metaManager
    // if (
    //   (eventName === "entity.create" || eventName === "entity.update" ||
    //     eventName === "entity.delete") &&
    //   payload.namespace === "meta"
    // ) {
    //   await this.configureApplication();
    // }
  }

  registerService(name: string, service: any) {
    this.#services.set(name, service);
  }

  getService<TService>(name: string): TService {
    return this.#services.get(name);
  }

  async start() {
    this.#logger.info("Starting rapid server...");
    const pluginManager = this.#pluginManager;
    await pluginManager.loadPlugins(this.#plugins);

    await pluginManager.initPlugins();

    await pluginManager.registerMiddlewares();
    await pluginManager.registerActionHandlers();
    await pluginManager.registerEventHandlers();
    await pluginManager.registerMessageHandlers();
    await pluginManager.registerTaskProcessors();

    this.#entityWatchers = this.#entityWatchers.concat(this.#appEntityWatchers);
    for (const entityWatcher of this.#entityWatchers) {
      if (entityWatcher.eventName === "entity.beforeCreate") {
        this.#entityBeforeCreateEventEmitters.on(entityWatcher.modelSingularCode, entityWatcher.handler);
      } else if (entityWatcher.eventName === "entity.create") {
        this.#entityCreateEventEmitters.on(entityWatcher.modelSingularCode, entityWatcher.handler);
      } else if (entityWatcher.eventName === "entity.beforeUpdate") {
        this.#entityBeforeUpdateEventEmitters.on(entityWatcher.modelSingularCode, entityWatcher.handler);
      } else if (entityWatcher.eventName === "entity.update") {
        this.#entityUpdateEventEmitters.on(entityWatcher.modelSingularCode, entityWatcher.handler);
      } else if (entityWatcher.eventName === "entity.beforeDelete") {
        this.#entityBeforeDeleteEventEmitters.on(entityWatcher.modelSingularCode, entityWatcher.handler);
      } else if (entityWatcher.eventName === "entity.delete") {
        this.#entityDeleteEventEmitters.on(entityWatcher.modelSingularCode, entityWatcher.handler);
      } else if (entityWatcher.eventName === "entity.addRelations") {
        this.#entityAddRelationsEventEmitters.on(entityWatcher.modelSingularCode, entityWatcher.handler);
      } else if (entityWatcher.eventName === "entity.removeRelations") {
        this.#entityRemoveRelationsEventEmitters.on(entityWatcher.modelSingularCode, entityWatcher.handler);
      } else if (entityWatcher.eventName === "entity.beforeResponse") {
        this.#entityBeforeResponseEventEmitters.on(entityWatcher.modelSingularCode, entityWatcher.handler);
      }
    }

    await this.configureApplication();

    this.#logger.info(`Rapid server ready.`);
    await pluginManager.onApplicationReady(this.#applicationConfig);
  }

  async configureApplication() {
    this.#applicationConfig = cloneDeep(this.#bootstrapApplicationConfig) as RpdApplicationConfig;

    const pluginManager = this.#pluginManager;
    await pluginManager.onLoadingApplication(this.#applicationConfig);
    await pluginManager.configureModels(this.#applicationConfig);
    await pluginManager.configureModelProperties(this.#applicationConfig);
    await pluginManager.configureServices(this.#applicationConfig);
    await pluginManager.configureRoutes(this.#applicationConfig);

    // TODO: check application configuration.

    await pluginManager.onApplicationLoaded(this.#applicationConfig);

    this.#buildedRoutes = await buildRoutes(this, this.#applicationConfig);
  }

  registerFacilityFactory(factory: FacilityFactory) {
    this.#facilityFactories.set(factory.name, factory);
  }

  async getFacility<TFacility = any>(name: string, options?: any, nullIfUnknownFacility?: boolean): Promise<TFacility> {
    const factory = this.#facilityFactories.get(name);
    if (!factory) {
      if (nullIfUnknownFacility) {
        return null;
      } else {
        throw new Error(`Failed to get facility. Unknown facility name: ${name}`);
      }
    }

    return await factory.createFacility(this, options);
  }

  async queryDatabaseObject(sql: string, params?: unknown[] | Record<string, unknown>): Promise<any[]> {
    try {
      return await this.#databaseAccessor.queryDatabaseObject(sql, params);
    } catch (err) {
      this.#logger.error("Failed to query database object.", { errorMessage: err.message, sql, params });
      throw err;
    }
  }

  async tryQueryDatabaseObject(sql: string, params?: unknown[] | Record<string, unknown>): Promise<any[]> {
    try {
      return await this.queryDatabaseObject(sql, params);
    } catch (err) {
      this.#logger.error("Failed to query database object.", { errorMessage: err.message, sql, params });
    }

    return [];
  }

  get middlewares() {
    return this.#middlewares;
  }

  async handleRequest(request: Request, next: Next) {
    const rapidRequest = new RapidRequest(this, request);
    await rapidRequest.parseBody();
    const routeContext: RouteContext = new RouteContext(this, rapidRequest);

    try {
      await this.#pluginManager.onPrepareRouteContext(routeContext);
      await this.#buildedRoutes(routeContext, next);
    } catch (ex) {
      this.#logger.error("handle request error:", ex);
      routeContext.response.json(
        {
          error: {
            message: ex.message || ex,
          },
        },
        500,
      );
    }
    return routeContext.response.getResponse();
  }

  async beforeRunRouteActions(handlerContext: ActionHandlerContext) {
    await this.#pluginManager.beforeRunRouteActions(handlerContext);
  }

  async beforeCreateEntity(model: RpdDataModel, options: CreateEntityOptions) {
    await this.#pluginManager.beforeCreateEntity(model, options);
  }

  async beforeUpdateEntity(model: RpdDataModel, options: UpdateEntityByIdOptions, currentEntity: any) {
    await this.#pluginManager.beforeUpdateEntity(model, options, currentEntity);
  }

  #handleEntityEvent(eventName: keyof RpdServerEventTypes, sender: RapidPlugin, payload: RpdEntityCreateEventPayload) {
    const { modelSingularCode, baseModelSingularCode } = payload;
    const entityWatchHandlerContext: EntityWatchHandlerContext<typeof eventName> = {
      server: this,
      payload,
    };

    let emitter: EventManager<Record<string, [EntityWatchHandlerContext<any>]>>;
    if (eventName === "entity.beforeCreate") {
      emitter = this.#entityBeforeCreateEventEmitters;
    } else if (eventName === "entity.create") {
      emitter = this.#entityCreateEventEmitters;
    } else if (eventName === "entity.beforeUpdate") {
      emitter = this.#entityBeforeUpdateEventEmitters;
    } else if (eventName === "entity.update") {
      emitter = this.#entityUpdateEventEmitters;
    } else if (eventName === "entity.beforeDelete") {
      emitter = this.#entityBeforeDeleteEventEmitters;
    } else if (eventName === "entity.delete") {
      emitter = this.#entityDeleteEventEmitters;
    } else if (eventName === "entity.addRelations") {
      emitter = this.#entityAddRelationsEventEmitters;
    } else if (eventName === "entity.removeRelations") {
      emitter = this.#entityRemoveRelationsEventEmitters;
    } else if (eventName === "entity.beforeResponse") {
      emitter = this.#entityBeforeResponseEventEmitters;
    }

    emitter.emit(modelSingularCode, entityWatchHandlerContext);
    if (baseModelSingularCode) {
      emitter.emit(baseModelSingularCode, entityWatchHandlerContext);
    }
  }
}
