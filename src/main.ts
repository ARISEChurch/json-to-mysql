import * as F from "fp-ts/function";
import * as O from "fp-ts/Option";
import * as Arr from "fp-ts/Array";
import * as Sql from "sqlstring";
import { DateTime } from "luxon";

export type TContextFields = {
  timeFields: string[];
  dateFields: string[];
};

const isNullable = (key: string, rows: any[]) =>
  rows.slice(0, 200).some((row) => row[key] == null);

const firstSample = (key: string, rows: any[]) =>
  F.pipe(
    rows.slice(0, 200),
    Arr.findFirst((row) => row[key] != null),
    O.map((row) => row[key]),
  );

const mysqlType = ({ timeFields, dateFields }: TContextFields) => (
  key: string,
) => (value: any): O.Option<string> => {
  if (timeFields.includes(key)) {
    return O.some("DATETIME");
  }
  if (dateFields.includes(key)) {
    return O.some("DATE");
  } else if (typeof value === "string") {
    return O.some("VARCHAR(255)");
  } else if (typeof value === "number") {
    return O.some("INT");
  } else if (typeof value === "boolean") {
    return O.some("BOOLEAN");
  }

  return O.none;
};

export const schemaFromSamples = (ctx: TContextFields) => (
  name: string,
  rows: any[],
) => {
  const columns = Object.keys(rows[0]);
  const hasID = columns.includes("id");
  const getType = mysqlType(ctx);

  const clauses = columns.reduce<string[]>(
    (clauses, name) =>
      F.pipe(
        firstSample(name, rows),
        O.chain(getType(name)),
        O.map((type) =>
          isNullable(name, rows)
            ? `${name} ${type}`
            : `${name} ${type} NOT NULL`,
        ),
        O.fold(
          () => clauses,
          (clause) => [...clauses, clause],
        ),
      ),
    [],
  );

  if (hasID) {
    clauses.push("PRIMARY KEY (id)");
  }

  return Sql.format(
    `CREATE TABLE ?? (
    ${clauses.join(",\n")}
  );`,
    [name],
  );
};

const valueForRow = ({ timeFields, dateFields }: TContextFields) => (
  row: any,
) => (column: string) => {
  const value = row[column];
  if (timeFields.includes(column)) {
    return F.pipe(
      O.fromNullable(value as DateTime),
      O.map((d) => d.toSQL()),
      O.toNullable,
    );
  } else if (dateFields.includes(column)) {
    return F.pipe(
      O.fromNullable(value as DateTime),
      O.map((d) => d.toUTC().toSQLDate()),
      O.toNullable,
    );
  }

  return value;
};

const valuesForRow = (ctx: TContextFields) => (columns: string[]) => {
  const placeholders = columns.map(() => "?").join(", ");
  const getValue = valueForRow(ctx);
  return (row: any) =>
    Sql.format(`(${placeholders})`, columns.map(getValue(row)));
};

export const insertMany = (ctx: TContextFields) => (
  table: string,
  rows: any[],
) => {
  const columns = Object.keys(rows[0]);
  const values = rows.map(valuesForRow(ctx)(columns));

  return Sql.format(
    `INSERT INTO ?? (${columns
      .map(() => "??")
      .join(", ")}) VALUES ${values.join(",\n")};`,
    [table, ...columns],
  );
};

export const dropTable = (table: string) =>
  Sql.format(`DROP TABLE IF EXISTS ??;`, [table]);
