import * as F from "fp-ts/lib/function";
import * as O from "fp-ts/lib/Option";
import * as Arr from "fp-ts/lib/Array";
import * as Sql from "sqlstring";
import { DateTime } from "luxon";

const isNullable = (key: string, rows: any[]) =>
  rows.some((row) => row[key] == null);

const firstSample = (key: string, rows: any[]) =>
  F.pipe(
    rows,
    Arr.findFirst((row) => row[key] != null),
    O.map((row) => row[key])
  );

const maxLength = (key: string, rows: any[]) =>
  Math.max(...rows.map((row) => row[key]?.length ?? 0));

const hasLongText = (key: string, rows: any[]) => maxLength(key, rows) > 255;

const mysqlType =
  ({
    timeFields,
    dateFields,
  }: Required<Pick<SchemaOpts, "dateFields" | "timeFields">>) =>
  (key: string) =>
  (value: any): SchemaKind => {
    if (timeFields.includes(key)) {
      return "DATETIME";
    } else if (dateFields.includes(key)) {
      return "DATE";
    } else if (typeof value === "string") {
      return "VARCHAR(255)";
    } else if (typeof value === "number") {
      return "INT";
    } else if (typeof value === "boolean") {
      return "BOOLEAN";
    }

    return "JSON";
  };

export interface SchemaOpts {
  name: string;
  samples?: number;
  timeZone?: string;
  timeFields?: string[];
  dateFields?: string[];
  allowJson?: boolean;
  setVarCharMaxLen?: boolean;
  rows: any[];
}

function shuffle<T>(input: T[]) {
  const array = [...input];

  let currentIndex = array.length,
    randomIndex;

  // While there remain elements to shuffle.
  while (currentIndex != 0) {
    // Pick a remaining element.
    randomIndex = Math.floor(Math.random() * currentIndex);
    currentIndex--;

    // And swap it with the current element.
    [array[currentIndex], array[randomIndex]] = [
      array[randomIndex],
      array[currentIndex],
    ];
  }

  return array;
}

export type SchemaKind =
  | "DATETIME"
  | "DATE"
  | `VARCHAR(${number})`
  | "TEXT"
  | "INT"
  | "BOOLEAN"
  | "JSON";

export interface SchemaField {
  name: string;
  kind: SchemaKind;
  nullable: boolean;
}

export interface Schema {
  name: string;
  timeZone: string;
  fields: SchemaField[];
}

export const schema = ({
  name,
  rows,
  samples = 500,
  dateFields = [],
  timeFields = [],
  allowJson = false,
  timeZone = "UTC",
  setVarCharMaxLen = false,
}: SchemaOpts): Schema => {
  const sampleSlice = shuffle(rows).slice(0, samples);
  const columns = [...new Set(sampleSlice.flatMap((row) => Object.keys(row)))];
  const getType = mysqlType({ dateFields, timeFields });

  const fields = columns.flatMap((name) =>
    F.pipe(
      F.pipe(
        firstSample(name, sampleSlice),
        O.map(getType(name)),
        O.filter((type) => type !== "JSON" || allowJson),
        O.map((type): SchemaKind => {
          if (type === "VARCHAR(255)") {
            if (hasLongText(name, rows)) {
              return "TEXT";
            } else if (setVarCharMaxLen) {
              return `VARCHAR(${maxLength(name, rows)})` as SchemaKind;
            }
          }

          return type;
        }),
        O.fold(
          () => [],
          (kind) => [
            {
              name,
              kind,
              nullable: isNullable(name, sampleSlice),
            },
          ]
        )
      )
    )
  );

  return { name, fields, timeZone };
};

export const createTable = ({ name, fields }: Schema) => {
  const hasID = fields.some((f) => f.name === "id");
  const clauses = fields.map((f) =>
    Sql.format(`?? ${f.kind}${f.nullable ? "" : " NOT NULL"}`, [f.name])
  );

  if (hasID) {
    clauses.push("PRIMARY KEY (id)");
  }

  return Sql.format(
    `CREATE TABLE ?? (
    ${clauses.join(",\n")}
  );`,
    [name]
  );
};

const valueForRow = (schema: Schema) => (row: any) => (field: SchemaField) => {
  const value = row[field.name];

  if (field.kind === "DATETIME") {
    return F.pipe(
      parseDateTime(value, schema.timeZone),
      O.map((d) => d.toSQL()),
      O.toNullable
    );
  } else if (field.kind === "DATE") {
    return F.pipe(
      parseDateTime(value, "UTC"),
      O.map((d) => d.toSQLDate()),
      O.toNullable
    );
  } else if (field.kind === "JSON") {
    return JSON.stringify(value);
  }

  return value;
};

const valuesForRow = (schema: Schema) => {
  const getValue = valueForRow(schema);
  const placeholders = schema.fields.map(() => "?").join(", ");

  return (row: any) =>
    Sql.format(`(${placeholders})`, schema.fields.map(getValue(row)));
};

export const insertMany = (schema: Schema) => (rows: any[]) => {
  const columns = schema.fields.map((f) => f.name);
  const values = rows.map(valuesForRow(schema));

  return Sql.format(
    `INSERT INTO ?? (${columns
      .map(() => "??")
      .join(", ")}) VALUES ${values.join(",\n")};`,
    [schema.name, ...columns]
  );
};

export const dropTable = (table: string) =>
  Sql.format(`DROP TABLE IF EXISTS ??;`, [table]);

const parseDateTime = (s: string, zone: string) =>
  F.pipe(
    O.fromNullable(DateTime.fromSQL(s, { zone })),
    O.filter((dt) => dt.isValid),
    O.alt(() => O.fromNullable(DateTime.fromISO(s)))
  );
