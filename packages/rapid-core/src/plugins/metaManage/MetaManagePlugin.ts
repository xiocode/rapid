/**
 * Meta manager plugin
 */

import {
  IQueryBuilder,
  QuoteTableOptions,
  RpdApplicationConfig,
  RpdDataModel,
  RpdDataModelIndex,
  RpdDataModelProperty,
  RpdDataPropertyTypes,
  RpdEntityCreateEventPayload,
  RpdEntityDeleteEventPayload,
  RpdEntityUpdateEventPayload,
} from "~/types";
import {
  IRpdServer,
  RapidPlugin,
  RpdConfigurationItemOptions,
  RpdServerPluginConfigurableTargetOptions,
  RpdServerPluginExtendingAbilities,
} from "~/core/server";

import * as listMetaModels from "./actionHandlers/listMetaModels";
import * as listMetaRoutes from "./actionHandlers/listMetaRoutes";
import * as getMetaModelDetail from "./actionHandlers/getMetaModelDetail";
import {find, isString, map} from "lodash";
import {
  getEntityPropertiesIncludingBase,
  getEntityPropertyByCode,
  isOneRelationProperty,
  isRelationProperty
} from "~/helpers/metaHelper";
import {DataAccessPgColumnTypes} from "~/dataAccess/dataAccessTypes";
import {pgPropertyTypeColumnMap} from "~/dataAccess/columnTypeMapper";

class MetaManager implements RapidPlugin {
  get code(): string {
    return "metaManager";
  }

  get description(): string {
    return null;
  }

  get extendingAbilities(): RpdServerPluginExtendingAbilities[] {
    return [];
  }

  get configurableTargets(): RpdServerPluginConfigurableTargetOptions[] {
    return [];
  }

  get configurations(): RpdConfigurationItemOptions[] {
    return [];
  }

  async registerActionHandlers(server: IRpdServer): Promise<any> {
    server.registerActionHandler(this, listMetaModels);
    server.registerActionHandler(this, listMetaRoutes);
    server.registerActionHandler(this, getMetaModelDetail);
  }

  async registerEventHandlers(server: IRpdServer): Promise<any> {
    server.registerEventHandler("entity.create", handleEntityCreateEvent.bind(this, server));
    server.registerEventHandler("entity.update", handleEntityUpdateEvent.bind(this, server));
    server.registerEventHandler("entity.delete", handleEntityDeleteEvent.bind(this, server));
  }

  async configureModels(server: IRpdServer, applicationConfig: RpdApplicationConfig): Promise<any> {
    const logger = server.getLogger();
    try {
      logger.info("Loading meta of models...");
      const models: RpdDataModel[] = await listCollections(server, applicationConfig);
      server.appendApplicationConfig({ models });
    } catch (error) {
      logger.crit("Failed to load meta of models.", { error });
    }
  }

  async onApplicationLoaded(server: IRpdServer, applicationConfig: RpdApplicationConfig): Promise<any> {
    await syncDatabaseSchema(server, applicationConfig);
  }
}

export default MetaManager;

async function handleEntityCreateEvent(server: IRpdServer, sender: RapidPlugin, payload: RpdEntityCreateEventPayload) {
  if (sender === this) {
    return;
  }

  if (payload.namespace === "meta" && payload.modelSingularCode === "model") {
    return;
    const { queryBuilder } = server;
    const model: Partial<RpdDataModel> = payload.after;
    if (model.tableName) {
      const model: RpdDataModel = payload.after;
      await server.queryDatabaseObject(`CREATE TABLE ${ queryBuilder.quoteTable(model) }
                                        (
                                        );`, []);
    }
  }
}

async function handleEntityUpdateEvent(server: IRpdServer, sender: RapidPlugin, payload: RpdEntityUpdateEventPayload) {
  if (sender === this) {
    return;
  }

  if (payload.namespace === "meta" && payload.modelSingularCode === "model") {
    return;
    const { queryBuilder } = server;
    const modelChanges: Partial<RpdDataModel> = payload.changes;
    if (modelChanges.tableName) {
      const modelBefore: RpdDataModel = payload.before;
      await server.queryDatabaseObject(
        `ALTER TABLE ${ queryBuilder.quoteTable(modelBefore) }
          RENAME TO ${ queryBuilder.quoteTable(modelChanges as QuoteTableOptions) }`,
        [],
      );
    }
  }
}

async function handleEntityDeleteEvent(server: IRpdServer, sender: RapidPlugin, payload: RpdEntityDeleteEventPayload) {
  if (sender === this) {
    return;
  }

  if (payload.namespace !== "meta") {
    return;
  }

  const { queryBuilder } = server;

  if (payload.modelSingularCode === "model") {
    const deletedModel: RpdDataModel = payload.before;
    await server.queryDatabaseObject(`DROP TABLE ${ queryBuilder.quoteTable(deletedModel) }`, []);
  } else if (payload.modelSingularCode === "property") {
    const deletedProperty: RpdDataModelProperty = payload.before;

    let columnNameToDrop = deletedProperty.columnName || deletedProperty.code;
    if (isRelationProperty(deletedProperty)) {
      if (deletedProperty.relation === "one") {
        columnNameToDrop = deletedProperty.targetIdColumnName || "";
      } else {
        // many relation
        return;
      }
    }

    const dataAccessor = server.getDataAccessor<RpdDataModel>({
      namespace: "meta",
      singularCode: "model",
    });
    const model = await dataAccessor.findById((deletedProperty as any).modelId);
    if (model) {
      await server.queryDatabaseObject(`ALTER TABLE ${ queryBuilder.quoteTable(model) }
        DROP COLUMN ${ queryBuilder.quoteObject(columnNameToDrop) }`, []);
    }
  }
}

function listCollections(server: IRpdServer, applicationConfig: RpdApplicationConfig) {
  const entityManager = server.getEntityManager("model");
  const model = entityManager.getModel();

  const properties = getEntityPropertiesIncludingBase(server, model);
  return entityManager.findEntities({
    properties: properties.map((item) => item.code),
  });
}

type TableInformation = {
  table_schema: string;
  table_name: string;
};

type ColumnInformation = {
  table_schema: string;
  table_name: string;
  column_name: string;
  data_type: string;
  udt_name: string;
  is_nullable: "YES" | "NO";
  column_default: string;
  character_maximum_length: number;
  numeric_precision: number;
  numeric_scale: number;
};

type ConstraintInformation = {
  table_schema: string;
  table_name: string;
  constraint_type: string;
  constraint_name: string;
};

async function syncDatabaseSchema(server: IRpdServer, applicationConfig: RpdApplicationConfig) {
  const logger = server.getLogger();
  logger.info("Synchronizing database schema...");
  const sqlQueryTableInformations = `SELECT table_schema, table_name
                                     FROM information_schema.tables`;
  const tablesInDb: TableInformation[] = await server.queryDatabaseObject(sqlQueryTableInformations);
  const { queryBuilder } = server;

  for (const model of applicationConfig.models) {
    logger.debug(`Checking data table for '${ model.namespace }.${ model.singularCode }'...`);

    const expectedTableSchema = model.schema || server.databaseConfig.dbDefaultSchema;
    const expectedTableName = model.tableName;
    const tableInDb = find(tablesInDb, { table_schema: expectedTableSchema, table_name: expectedTableName });
    if (!tableInDb) {
      await server.queryDatabaseObject(`CREATE TABLE IF NOT EXISTS ${ queryBuilder.quoteTable(model) }
                                        (
                                        )`, []);
    }
  }

  const sqlQueryColumnInformations = `SELECT table_schema,
                                             table_name,
                                             column_name,
                                             data_type,
                                             udt_name,
                                             is_nullable,
                                             column_default,
                                             character_maximum_length,
                                             numeric_precision,
                                             numeric_scale
                                      FROM information_schema.columns;`;
  const columnsInDb: ColumnInformation[] = await server.queryDatabaseObject(sqlQueryColumnInformations, []);

  for (const model of applicationConfig.models) {
    logger.debug(`Checking data columns for '${ model.namespace }.${ model.singularCode }'...`);

    // alter table public.base_locations
    // add constraint fk_base_locations_base_locations foreign key (parent_id) references public.base_locations (id) on delete cascade ;

    // alter table public.base_locations
    // add warehouse_id integer
    // constraint fk_base_locations_warehouse_id references public.base_warehouses (id) on delete cascade;

    for (const property of model.properties) {
      let columnDDL = "";
      if (isRelationProperty(property)) {
        if (property.relation === "one") {
          const targetModel = applicationConfig.models.find((item) => item.singularCode === property.targetSingularCode);
          if (!targetModel) {
            logger.warn(`Cannot find target model with singular code "${ property.targetSingularCode }".`);
          }
          const columnInDb: ColumnInformation | undefined = find(columnsInDb, {
            table_schema: model.schema || "public",
            table_name: model.tableName,
            column_name: property.targetIdColumnName!,
          });

          if (!columnInDb) {
            columnDDL = generateCreateColumnDDL(queryBuilder, {
              schema: model.schema,
              tableName: model.tableName,
              name: property.targetIdColumnName!,
              type: "integer",
              autoIncrement: false,
              notNull: property.required,
              isForeignKey: property.isForeignKey,
              targetTableName: targetModel.tableName,
            });
          }
        } else if (property.relation === "many") {
          if (property.linkTableName) {
            const tableInDb = find(tablesInDb, {
              table_schema: property.linkSchema || server.databaseConfig.dbDefaultSchema,
              table_name: property.linkTableName,
            });
            if (!tableInDb) {
              columnDDL = generateLinkTableDDL(queryBuilder, {
                linkSchema: property.linkSchema,
                linkTableName: property.linkTableName,
                targetIdColumnName: property.targetIdColumnName!,
                selfIdColumnName: property.selfIdColumnName!,
              });
            }

            const constraintName = `${ property.linkTableName }_pk`;
            columnDDL += `ALTER TABLE ${ queryBuilder.quoteTable({
              schema: property.linkSchema,
              tableName: property.linkTableName,
            }) }
              ADD CONSTRAINT ${ queryBuilder.quoteObject(constraintName) } PRIMARY KEY (id);`;
          } else {
            const targetModel = applicationConfig.models.find((item) => item.singularCode === property.targetSingularCode);
            if (!targetModel) {
              logger.warn(`Cannot find target model with singular code "${ property.targetSingularCode }".`);
              continue;
            }

            const columnInDb: ColumnInformation | undefined = find(columnsInDb, {
              table_schema: targetModel.schema || "public",
              table_name: targetModel.tableName,
              column_name: property.selfIdColumnName!,
            });

            if (!columnInDb) {
              columnDDL = generateCreateColumnDDL(queryBuilder, {
                schema: targetModel.schema,
                tableName: targetModel.tableName,
                name: property.selfIdColumnName || "",
                type: "integer",
                autoIncrement: false,
                notNull: property.required,
              });
            }
          }
        } else {
          continue;
        }

        if (columnDDL) {
          await server.tryQueryDatabaseObject(columnDDL);
        }
      } else {
        const columnName = property.columnName || property.code;
        const columnInDb: ColumnInformation | undefined = find(columnsInDb, {
          table_schema: model.schema || "public",
          table_name: model.tableName,
          column_name: columnName,
        });

        if (property.isForeignKey) {
          throw new Error("only relation property can be foreign key");
        }

        if (!columnInDb) {
          // create column if not exists
          columnDDL = generateCreateColumnDDL(queryBuilder, {
            schema: model.schema,
            tableName: model.tableName,
            name: columnName,
            type: property.type,
            autoIncrement: property.autoIncrement,
            notNull: property.required,
            defaultValue: property.defaultValue,
          });
          await server.tryQueryDatabaseObject(columnDDL);
        } else {
          const expectedColumnType = pgPropertyTypeColumnMap[property.type];
          if (columnInDb.udt_name !== expectedColumnType) {
            const sqlAlterColumnType = `alter table ${ queryBuilder.quoteTable(model) }
              alter column ${ queryBuilder.quoteObject(
                columnName,
              ) } type ${ expectedColumnType }`;
            await server.tryQueryDatabaseObject(sqlAlterColumnType);
          }

          if (property.defaultValue) {
            if (!columnInDb.column_default) {
              const sqlSetColumnDefault = `alter table ${ queryBuilder.quoteTable(model) }
                alter column ${ queryBuilder.quoteObject(columnName) } set default ${
                  property.defaultValue
                }`;
              await server.tryQueryDatabaseObject(sqlSetColumnDefault);
            }
          } else {
            if (columnInDb.column_default && !property.autoIncrement) {
              const sqlDropColumnDefault = `alter table ${ queryBuilder.quoteTable(model) }
                alter column ${ queryBuilder.quoteObject(columnName) } drop default`;
              await server.tryQueryDatabaseObject(sqlDropColumnDefault);
            }
          }

          if (property.required) {
            if (columnInDb.is_nullable === "YES") {
              const sqlSetColumnNotNull = `alter table ${ queryBuilder.quoteTable(model) }
                alter column ${ queryBuilder.quoteObject(columnName) } set not null`;
              await server.tryQueryDatabaseObject(sqlSetColumnNotNull);
            }
          } else {
            if (columnInDb.is_nullable === "NO") {
              const sqlDropColumnNotNull = `alter table ${ queryBuilder.quoteTable(model) }
                alter column ${ queryBuilder.quoteObject(columnName) } drop not null`;
              await server.tryQueryDatabaseObject(sqlDropColumnNotNull);
            }
          }
        }
      }
    }
  }

  const sqlQueryConstraints = `SELECT table_schema, table_name, constraint_type, constraint_name
                               FROM information_schema.table_constraints
                               WHERE constraint_type = 'PRIMARY KEY';`;
  const constraintsInDb: ConstraintInformation[] = await server.queryDatabaseObject(sqlQueryConstraints);
  for (const model of applicationConfig.models) {
    const expectedTableSchema = model.schema || server.databaseConfig.dbDefaultSchema;
    const expectedTableName = model.tableName;
    const expectedContraintName = `${ expectedTableName }_pk`;
    logger.debug(`Checking pk for '${ expectedTableSchema }.${ expectedTableName }'...`);
    const constraintInDb = find(constraintsInDb, {
      table_schema: expectedTableSchema,
      table_name: expectedTableName,
      constraint_type: "PRIMARY KEY",
      constraint_name: expectedContraintName,
    });
    if (!constraintInDb) {
      await server.queryDatabaseObject(
        `ALTER TABLE ${ queryBuilder.quoteTable(model) }
          ADD CONSTRAINT ${ queryBuilder.quoteObject(expectedContraintName) } PRIMARY KEY (id);`,
        [],
      );
    }
  }

  // generate indexes
  for (const model of applicationConfig.models) {
    if (!model.indexes || !model.indexes.length) {
      continue;
    }

    for (const index of model.indexes) {
      const sqlCreateIndex = generateTableIndexDDL(queryBuilder, server, model, index);
      await server.tryQueryDatabaseObject(sqlCreateIndex, []);
    }
  }
}

function generateCreateColumnDDL(
  queryBuilder: IQueryBuilder,
  options: {
    schema?: string;
    tableName: string;
    name: string;
    type: RpdDataPropertyTypes;
    autoIncrement?: boolean;
    notNull?: boolean;
    defaultValue?: string;
    isForeignKey?: boolean;
    targetTableName?: string;
  },
) {
  let columnDDL = `ALTER TABLE ${ queryBuilder.quoteTable(options) }
    ADD`;
  columnDDL += ` ${ queryBuilder.quoteObject(options.name) }`;
  if (options.type === "integer" && options.autoIncrement) {
    columnDDL += ` serial`;
  } else {
    const columnType = pgPropertyTypeColumnMap[options.type];
    if (!columnType) {
      throw new Error(`Property type "${ options.type }" is not supported.`);
    }
    columnDDL += ` ${ columnType }`;
  }
  if (options.notNull) {
    columnDDL += " NOT NULL";
  }

  if (options.defaultValue) {
    columnDDL += ` DEFAULT ${ options.defaultValue }`;
  }

  if (options.isForeignKey && options.targetTableName) {
    columnDDL += ` CONSTRAINT fk_${ options.tableName }_${ options.name } REFERENCES ${ queryBuilder.quoteTable({ tableName: options.targetTableName }) } (id) ON DELETE CASCADE `;
  }

  return columnDDL;
}

function generateLinkTableDDL(
  queryBuilder: IQueryBuilder,
  options: {
    linkSchema?: string;
    linkTableName: string;
    targetIdColumnName: string;
    selfIdColumnName: string;
  },
) {
  let columnDDL = `CREATE TABLE ${ queryBuilder.quoteTable({
    schema: options.linkSchema,
    tableName: options.linkTableName,
  }) }
                   (`;
  columnDDL += `id serial not null,`;
  columnDDL += `${ queryBuilder.quoteObject(options.selfIdColumnName) } integer not null,`;
  columnDDL += `${ queryBuilder.quoteObject(options.targetIdColumnName) } integer not null);`;

  return columnDDL;
}

function generateTableIndexDDL(queryBuilder: IQueryBuilder, server: IRpdServer, model: RpdDataModel, index: RpdDataModelIndex) {
  let indexName = index.name;
  if (!indexName) {
    indexName = model.tableName;
    for (const indexProp of index.properties) {
      const propCode = isString(indexProp) ? indexProp : indexProp.code;
      const property = getEntityPropertyByCode(server, model, propCode);
      if (!isRelationProperty(property)) {
        indexName += "_" + property.columnName;
      } else if (isOneRelationProperty(property)) {
        indexName += "_" + property.targetIdColumnName;
      }
    }
    indexName += index.unique ? "_uindex" : "_index";
  }

  const indexColumns = map(index.properties, (indexProp) => {
    let columnName: string;
    const propCode = isString(indexProp) ? indexProp : indexProp.code;
    const property = getEntityPropertyByCode(server, model, propCode);
    if (!isRelationProperty(property)) {
      columnName = property.columnName;
    } else if (isOneRelationProperty(property)) {
      columnName = property.targetIdColumnName;
    }

    if (isString(indexProp)) {
      return columnName;
    }

    if (indexProp.order === "desc") {
      return `${ columnName } desc`;
    }

    return columnName;
  });

  let ddl = `CREATE
  ${ index.unique ? "UNIQUE" : "" } INDEX
  ${ indexName } `;
  ddl += `ON ${ queryBuilder.quoteTable({
    schema: model.schema,
    tableName: model.tableName,
  }) } (${ indexColumns.join(", ") })`;

  if (index.conditions) {
    ddl += ` WHERE ${ queryBuilder.buildFiltersExpression(model, index.conditions) }`;
  }

  return ddl;
}
