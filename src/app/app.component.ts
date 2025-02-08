import { Component, OnInit, signal } from '@angular/core';
import { RouterOutlet } from '@angular/router';
import * as duckdb from '@duckdb/duckdb-wasm';
import * as arrow from 'apache-arrow';

@Component({
  selector: 'app-root',
  imports: [RouterOutlet],
  templateUrl: './app.component.html',
  styleUrl: './app.component.css',
})
export class AppComponent {
  constructor() {
    this.initDb();
  }
  title = 'angular-duck-db-v2';
  db: duckdb.AsyncDuckDB | undefined;

  numberRows = signal(0);

  async initDb() {
    console.log('The process start on', new Date());
    const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();

    // Select a bundle based on browser checks
    const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

    const worker_url = URL.createObjectURL(
      new Blob([`importScripts("${bundle.mainWorker!}");`], {
        type: 'text/javascript',
      })
    );

    // Instantiate the asynchronus version of DuckDB-wasm
    const worker = new Worker(worker_url);
    const logger = new duckdb.ConsoleLogger();
    this.db = new duckdb.AsyncDuckDB(logger, worker);
    await this.db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    URL.revokeObjectURL(worker_url);
    console.log('The process finish on', new Date());
  }

  async ingestData() {
    const c = await this.db?.connect();
    const csvContent = '1,name\n2,age\n';
    await this.db?.registerFileText(`data.csv`, csvContent);
    // ... with typed insert options
    await c?.insertCSVFromPath('data.csv', {
      schema: 'main',
      name: 'erazosV2',
      detect: false,
      header: false,
      delimiter: ',',
      columns: {
        col1: new arrow.Utf8(),
        col2: new arrow.Utf8(),
      },
    });
  }

  async ingestFromParquet() {
    const c = await this.db?.connect();
    await this.db?.registerFileURL(
      'file.parquet',
      'http://localhost:8000/file.parquet',
      duckdb.DuckDBDataProtocol.HTTP,
      false
    );

    c?.close();
  }

  async queryData() {
    const conn = await this.db?.connect();
    const result = await conn?.query<{ v: any }>(`SELECT * from daneradev`);

    console.log(result?.toArray());

    conn?.close();
  }
}
