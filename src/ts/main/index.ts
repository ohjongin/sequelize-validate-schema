/**
 * originated from https://github.com/hiradimir/sequelize-validate-schema
 */

import assertCore from 'assert';
import process from 'process';
import * as Sequelize from 'sequelize';
import * as _ from 'lodash';
import assert from 'assert-fine';
import dayjs from 'dayjs';
import { Mutex } from 'async-mutex';
import { OrderStates } from '../common/state.type';
import * as TIME from '../common/time';
import { initGlobals } from '../common/globals';
import { getCaller, isMasterNode, sleep, terminate } from './utils';
import logger from './logger';

(() => {
    if (!global.env) initGlobals();
})();

const env = global.env;
const mutex = new Mutex();

interface IModelAttribute {
    type: any;
    primaryKey: boolean;
    autoIncrement: boolean;
    field: string;
    allowNull: boolean;
    unique: boolean;
    name: string;
    onDelete: string;
    onUpdate: string;
    references?: { model: string, key: string };
}

interface IRawModel {
    attributes: { [index: string]: IModelAttribute };
    primaryKeys: { [index: string]: IModelAttribute };
    options: any;
}

// @ts-ignore
interface IDescribedAttribute {
    type: string;
    allowNull: boolean;
    defaultValue: any;
    primaryKey: boolean;
}

const dataTypeToDBTypeDialect: {
    [index: string]: (attr: IModelAttribute) => string
} = {
    postgres: (attr: IModelAttribute) => {
        // this support only postgres
        if (attr.type instanceof Sequelize.STRING) {
            // @ts-ignore
            return `CHARACTER VARYING(${attr.type._length})`;
        } else if (attr.type instanceof Sequelize.BIGINT) {
            return 'BIGINT';
        } else if (attr.type instanceof Sequelize.INTEGER) {
            return 'INTEGER';
        } else if (attr.type instanceof Sequelize.DATE) {
            return 'TIMESTAMP WITH TIME ZONE';
        } else if (attr.type instanceof <any>Sequelize.DATEONLY) {
            return 'DATE';
        } else {
            console.error(`${attr.field} is not support schema type.\n${JSON.stringify(attr)}`);
        }
        return undefined;
    },
    mysql: (attr: IModelAttribute) => {
        if (attr.type instanceof Sequelize.CHAR) {
            // @ts-ignore
            return `CHAR(${attr.type._length})`;
        } else if (attr.type instanceof Sequelize.STRING) {
            // @ts-ignore
            return `VARCHAR(${attr.type._length})`;
        } else if (attr.type instanceof Sequelize.BIGINT) {
            return 'BIGINT';
        } else if (attr.type instanceof Sequelize.TINYINT) {
            return 'TINYINT';
        } else if (attr.type instanceof Sequelize.INTEGER) {
            return 'INT';
        } else if (attr.type instanceof <any>Sequelize.DATEONLY) {
            return 'DATE';
        } else if (attr.type instanceof Sequelize.DATE) {
            return 'DATETIME';
        } else if (attr.type instanceof Sequelize.TEXT) {
            return 'TEXT';
        } else if (attr.type instanceof Sequelize.JSON) {
            return 'JSON';
        } else if (attr.type instanceof Sequelize.BOOLEAN) {
            return 'TINYINT(1)';
        } else if (attr.type instanceof Sequelize.DECIMAL) {
            // @ts-ignore
            return `DECIMAL(${attr.type._precision},${attr.type._scale})`;
        } else if (attr.type instanceof Sequelize.UUID) {
            // https://www.rfc-editor.org/rfc/rfc4122
            // 36 characters
            return 'VARCHAR(40)';
        } else {
            console.error(`${attr.field} is not support schema type.\n${JSON.stringify(attr)}`);
        }
        return undefined;
    },
};

assert.use(assertCore);
assert.beforeThrow(() => {  //  This call is optional.
    return undefined;              //  The breakpoint place.
});

export const each = async function(arr, fn) { // take an array and a function
    for(const item of arr) await fn(item);
}

/**
 * Validate schema of models.
 *
 * @param {Object} [options={}]
 * @param {String[]|function} [options.exclude=[`sequelizeMeta`]] if you want to skip validate table.
 * @param {Boolean|function} [options.logging=console.log] A function that logs sql queries, or false for no logging
 * @return {Promise}
 */
export const validateSchemas = async (sequelize, options?) => {
    console.log(dayjs().format(), getCaller(), '[validateSchemas]', `master:${isMasterNode()}, id:${process.pid}, validated: ${global.isSchemaValidated}`, getCaller(3));
    if (global.isSchemaValidated) return true;

    const release = await mutex.acquire();
    global.isSchemaValidated = true;

    let isSuccess = false;
    try {
        isSuccess = await validateSchemasWorker(sequelize, options);
    } catch (e) {
        console.error(dayjs().format, getCaller(), e);
        logger.error(e);
    } finally {
        release();
    }

    return isSuccess;
}

export const validateSchemasWorker = async (sequelize, options?) => {
    console.log(dayjs().format(), getCaller(), '[validateSchemasWorker]', `master:${isMasterNode()}, id:${process.pid},`, getCaller(3));

    options = _.clone(options) || {};
    options = _.defaults(options, { exclude: ['SequelizeMeta', 'tags'] }, sequelize.options);

    const queryInterface = sequelize.getQueryInterface();

    // @ts-ignore
    const dataTypeToDBType = dataTypeToDBTypeDialect[sequelize.options.dialect];

    const checkAttributes = async (queryInterface, tableName, model, options) => {
        const table = await queryInterface.describeTable(tableName, options);
        const columnNames = Object.keys(table);
        for (const fieldName of columnNames) {
            const attribute = table[fieldName];
            const modelAttr = model?.tableAttributes[fieldName];
            if (!modelAttr || _.isUndefined(modelAttr)) {
                console.log('fieldName:', fieldName);
                console.log('attributes:', JSON.stringify(columnNames || {}, null, 2));
                console.log('model?.tableAttributes:', JSON.stringify(model?.tableAttributes || {}, null, 2));
                console.log('modelAttr:', modelAttr);
                console.log('model.attributes:', JSON.stringify(model?.attributes || {}, null, 2));
            }

            assert(!_.isUndefined(modelAttr), `${tableName}.${fieldName} is not defined.\nmodelAttr:${modelAttr}.\nmodel.attributes:${JSON.stringify(model.attributes, null, 2)}`);
            const dataType = dataTypeToDBType(modelAttr);
            if (dataType !== attribute.type) {
                console.log('dataType:', dataType);
                console.log('attribute.type:', attribute.type);
                dataTypeToDBType(modelAttr);
            }

            assert(dataType === attribute.type, `${tableName}.${fieldName} field type is invalid.  Model.${fieldName}.type[${dataType}] != Table.${fieldName}.type[${attribute.type}]`);
            assert(modelAttr.field === fieldName, `fieldName is not same. Model.field[${modelAttr.field}] != Table.primaryKey[${attribute.primaryKey}]`);
            assert(modelAttr.primaryKey === true === attribute.primaryKey === true, `illegal primaryKey defined ${tableName}.${fieldName}. Model.primaryKey[${modelAttr.primaryKey}] != Table.primaryKey[${fieldName}]`);
            assert((modelAttr.allowNull === true || _.isUndefined(modelAttr.allowNull)) === attribute.allowNull === true, `illegal allowNull defined ${tableName}.${fieldName}. Model.allowNull[${modelAttr.allowNull}] != Table.allowNull[${attribute.allowNull}]`);
        }

        return table;
    };

    const checkForeignKey = async (queryInterface, tableName, model, options) => {
        const foreignKeys = await sequelize.query(queryInterface.queryGenerator.getForeignKeysQuery(tableName), options);

        for (const fk of foreignKeys) {
            if (sequelize.options.dialect === 'mysql') {
                // sequelize does not support to get foreignkey info at mysql
                return;
            }
            const modelAttr: IModelAttribute = model.attributes[fk.from.split('\"').join('')];
            assert(!_.isUndefined(modelAttr.references), `${tableName}.[${modelAttr.field}] must be defined foreign key.\n${JSON.stringify(fk, null, 2)}`);
            assert(fk.to === modelAttr.references.key, `${tableName}.${modelAttr.field} => ${modelAttr.references.key} must be same to foreignKey [${fk.to}].\n${JSON.stringify(fk, null, 2)}`);
        }
    };

    const checkIndexes = async (queryInterface, tableName, model: IRawModel, options) => {
        const indexes = await queryInterface.showIndex(tableName, options);
        for (const index of indexes) {
            if (index.primary) {
                index.fields.forEach(field => {
                    assert(!_.isUndefined(model.primaryKeys[field.attribute]), `${tableName}.${field.attribute} must be primaryKey`);
                });
            } else {
                const indexFields = _.map(index.fields, (field: any) => {
                    return field.attribute;
                });

                const modelIndex = model.options?.indexes?.find((mi: any) => {
                    const modelIndexFields = mi.fields?.map((field: any) => {
                        return typeof field === 'string' ? field : field.name;
                    });
                    return _.isEqual(modelIndexFields.sort(), indexFields.sort());
                });

                if (indexFields.length > 1) {
                    assert(!_.isUndefined(modelIndex), `${tableName}.[${indexFields}] must be defined combination key\n${JSON.stringify(index, null, 2)}`);
                }

                // @ts-ignore
                const attributes = model.rawAttributes;
                if (modelIndex) {
                    assert(modelIndex.unique === true === index.unique, `${tableName}.[${indexFields}] must be same unique value\n${JSON.stringify(index, null, 2)}`);
                } else if (attributes[indexFields[0]] && attributes[indexFields[0]].unique) {
                    assert(index.unique === true, `${tableName}.[${indexFields}] must be defined unique key\n${JSON.stringify(index, null, 2)}`);
                } else if (attributes[indexFields[0]] && attributes[indexFields[0]].references) {
                    // mysql create index with foreignKey
                    assert(sequelize.options.dialect === 'mysql', `${tableName}.[${indexFields}] is auto created index by mysql.\n${JSON.stringify(index, null, 2)}`);
                } else {
                    assert(false, `${tableName}.[${indexFields}] is not defined index.${JSON.stringify(index, null, 2)}`);
                }
            }
        }
    };

    const tables = await queryInterface.showAllTables(options);
    const models = tables
        .sort()
        .filter(tableName => {
            // treat exclude as a function
            return !_.includes(options.exclude, tableName);
        })
        .filter(tableName => {
            return sequelize.modelManager.models.find(m => m.tableName === tableName);
        })
        .map(tableName => {
            const model = sequelize.modelManager.models.find(m => m.tableName === tableName);
            return model;
        });

    let result = true;
    for (let i = 0; i < models?.length; i++) {
        const model = models[i];
        if (!model) {
            continue;
        }

        try {
            await checkAttributes(queryInterface, model.tableName, model, options);

            if (model.tableName === 'orders') {
                const table = await queryInterface.describeTable(model.tableName, options);
                const columnNames = Object.keys(table).filter(c => c.includes('_at')).filter(c => ![ 'touched_at', 'issued_at', 'updated_at', 'deleted_at', 'created_at', 'inspected_at' ].includes(c));
                const stateNames = OrderStates.map(s => s.dt_column).filter(s => s !== undefined);

                const uniqueColumnNames = _.uniqWith(columnNames);
                const uniqueStateNames = _.uniqWith(stateNames);

                uniqueColumnNames.sort();
                uniqueStateNames.sort();

                assert(_.isEqual(uniqueColumnNames, uniqueStateNames), `There are different date field between ${model.tableName} and order states.\ntable: ${JSON.stringify(uniqueColumnNames)}\nstates:${JSON.stringify(uniqueStateNames)}`);
            }
        } catch (e) {
            console.error(e);
            result = false;
        }
    }

    for (let i = 0; i < models?.length; i++) {
        const model = models[i];
        if (!model) {
            continue;
        }

        try {
            await checkForeignKey(queryInterface, model.tableName, model, options);
        } catch (e) {
            console.error(e);
            result = false;
        }
    }

    for (let i = 0; i < models?.length; i++) {
        const model = models[i];
        if (!model) {
            continue;
        }

        try {
            await checkIndexes(queryInterface, model.tableName, model, options);
        } catch (e) {
            console.error(e);
            result = false;
        }
    }

    // DB에는 있으나 Code에서 정의하고 있지 않은 Table 검사
    tables
        .sort()
        .forEach(tableName => {
            const result = sequelize.modelManager.models.find(m => m.tableName === tableName);

            // DB에는 있으나 Code에서 정의하고 있지 않은 Table은 제거
            if (!result) {
                console.log(`A Model('${tableName}') is not defined. Table(${tableName}) exists on DB.`);
            }

            return result;
        });

    console.log(`Schema validation is ${result ? 'success' : 'failed'}..`);

    if (!result) {
        console.log(`\n\n\n\n`);
        console.log(`Schema validation is ${result ? 'success' : 'failed'}..`);
        console.log(`\n\n`);
        console.log(`=-✗=-✗=-✗=-✗=-✗=-✗=-✗=-✗=-✗=-✗=-✗=-✗=-✗=-✗=-✗=-✗=-✗=-✗=-✗=-✗=-✗=-✗=-✗=-✗`);
        await sleep(TIME.SECOND * 60);

        if (env.mode.test) {
            terminate(1);
        }
    }
    console.log(`==========================================================================`);

    return result;
};
