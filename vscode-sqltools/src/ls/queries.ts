import { IBaseQueries } from '@sqltools/types';

/**
 * Keboola driver queries.
 *
 * Unlike traditional SQL drivers, Keboola uses REST APIs (Storage API) for
 * metadata/explorer operations rather than SQL introspection queries.
 * Most explorer functionality is implemented directly in the driver via HTTP calls.
 *
 * These query definitions satisfy the SQLTools IBaseQueries interface but are
 * intentionally minimal -- the driver overrides getChildrenForItem() to use
 * the Storage API instead of executing SQL.
 */

const describeTable: IBaseQueries['describeTable'] = {
  query: `SELECT 1`,
  rawQuery: `SELECT 1`,
};

const countRecords: IBaseQueries['countRecords'] = {
  query: `SELECT 1`,
  rawQuery: `SELECT 1`,
};

const fetchColumns: IBaseQueries['fetchColumns'] = {
  query: `SELECT 1`,
  rawQuery: `SELECT 1`,
};

const fetchRecords: IBaseQueries['fetchRecords'] = {
  query: `SELECT * FROM $1 LIMIT $2`,
  rawQuery: `SELECT * FROM $1 LIMIT $2`,
};

const fetchTables: IBaseQueries['fetchTables'] = {
  query: `SELECT 1`,
  rawQuery: `SELECT 1`,
};

const fetchViews: IBaseQueries['fetchViews'] = {
  query: `SELECT 1`,
  rawQuery: `SELECT 1`,
};

const fetchSchemas: IBaseQueries['fetchSchemas'] = {
  query: `SELECT 1`,
  rawQuery: `SELECT 1`,
};

const fetchDatabases: IBaseQueries['fetchDatabases'] = {
  query: `SELECT 1`,
  rawQuery: `SELECT 1`,
};

const searchTables: IBaseQueries['searchTables'] = {
  query: `SELECT 1`,
  rawQuery: `SELECT 1`,
};

const searchColumns: IBaseQueries['searchColumns'] = {
  query: `SELECT 1`,
  rawQuery: `SELECT 1`,
};

export default {
  describeTable,
  countRecords,
  fetchColumns,
  fetchRecords,
  fetchTables,
  fetchViews,
  fetchSchemas,
  fetchDatabases,
  searchTables,
  searchColumns,
};
