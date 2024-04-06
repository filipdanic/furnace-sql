export interface IOptions {
  processingMode?: "serial" | "parallel";
  debug?: boolean;
}

export interface ITable {
  name: string;
  schema: {
    key: string;
    type: "TEXT" | "INTEGER";
  }[];
  files: {
    type: "csv" | "parquet";
    path: string;
  }[];
}

export interface ITransformationConfig {
  transformationScript: string;
  tables: ITable[];
}

export interface ITableNext {
  name: string;
  schema: {
    key: string;
    type: "TEXT" | "INTEGER";
  }[];
  files: {
    file: {
      location: "local";
      path: string;
    };
    config: {
      type: "csv";
      delimiter: string;
    };
  }[];
}
