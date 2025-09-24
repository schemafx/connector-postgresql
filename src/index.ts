import type { Client } from 'pg';
import format from 'pg-format';
import Pool from 'pg-pool';
import { Connector, type TableDefinition } from 'schemafx';

export interface PostgreSQLConnectorOptions {
    /** Name for the connector. Defaults to "postgresql". */
    name?: string;
}

export default class PostgreSQLConnector extends Connector {
    /** A map to hold connection pools for different databases. */
    private pools = new Map<string, Pool<Client>>();

    /**
     * Build With PostgreSQL.
     * @param opts Configuration for PostgreSQL.
     */
    constructor(opts?: PostgreSQLConnectorOptions) {
        super(opts?.name || 'postgresql');

        this.authType = 'Basic';
        this.authProps = {
            host: 'Text',
            port: 'Number',
            user: 'Text',
            password: 'Password',
            database: 'Text',
            certificate: 'Password'
        };
    }

    /**
     * Executes a query on the database.
     * A new client is created for each query to ensure proper resource management
     * and handling of concurrent connections.
     * @param connectionPayload The authentication payload.
     * @param query The SQL query to execute.
     * @param values The values to pass to the query.
     * @returns The query result.
     */
    private async executeQuery(
        connectionPayload: Record<string, string>,
        query: string,
        values: unknown[] = []
    ): Promise<Record<string, unknown>[]> {
        const { host, port, user, password, database, certificate } = connectionPayload;
        const clientCredentials = {
            host,
            port: parseInt(port),
            user,
            password,
            database,
            ...(certificate ? { ssl: { ca: certificate } } : {})
        };

        const poolKey = JSON.stringify([
            clientCredentials.host,
            clientCredentials.port,
            clientCredentials.user,
            clientCredentials.password,
            clientCredentials.database,
            clientCredentials.ssl?.ca
        ]);

        let pool = this.pools.get(poolKey);
        if (!pool) {
            pool = new Pool(clientCredentials);
            this.pools.set(poolKey, pool);
        }

        let client;
        try {
            client = await pool.connect();
            return (await client.query(query, values)).rows;
        } catch (error) {
            console.error('Database query failed:', error);
            throw new Error('An error occurred while executing the database query.');
        } finally {
            if (client) {
                client.release();
            }
        }
    }

    /**
     * Maps SchemaFX data types to PostgreSQL data types.
     * @param type The SchemaFX data type.
     * @returns The corresponding PostgreSQL data type.
     */
    private mapToPostgresType(type: 'string' | 'number' | 'date' | 'datetime'): string {
        switch (type) {
            case 'string':
                return 'TEXT';
            case 'number':
                return 'DOUBLE PRECISION';
            case 'date':
                return 'DATE';
            case 'datetime':
                return 'TIMESTAMP';
            default:
                throw new Error(`Unsupported data type: ${type}`);
        }
    }

    /**
     * Read available tables.
     * @param connectionPath Connection path to explore.
     * @param connectionPayload Connection payload for auth.
     * @returns Available tables at the requested path.
     */
    async readTables(
        connectionPath: string[],
        connectionPayload: Record<string, string>
    ): Promise<{ name: string; connectionPath: string[]; final: boolean }[]> {
        return (
            await this.executeQuery(
                connectionPayload,
                "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'"
            )
        ).map(row => ({
            name: row.tablename as string,
            connectionPath: [row.tablename as string],
            final: true
        }));
    }

    /**
     * Read a table definition from its path.
     * @param connectionPath Connection path to consider.
     * @param connectionPayload Connection payload for auth.
     * @returns Understood table definition.
     */
    async readTable(
        connectionPath: string[],
        connectionPayload: Record<string, string>
    ): Promise<TableDefinition> {
        const tableName = connectionPath[0];
        const columns = (
            await this.executeQuery(
                connectionPayload,
                `
                    SELECT
                        a.attname AS column_name,
                        format_type(a.atttypid, a.atttypmod) AS data_type,
                        coalesce(i.indisprimary, false) AS is_primary_key
                    FROM pg_attribute a
                    JOIN pg_class c ON c.oid = a.attrelid
                    LEFT JOIN pg_index i ON i.indrelid = a.attrelid AND a.attnum = any(i.indkey)
                    WHERE c.relname = $1 AND a.attnum > 0;
                `,
                [tableName]
            )
        ).map(row => {
            let type: 'string' | 'number' | 'date' | 'datetime' = 'string';
            switch (row.data_type) {
                case 'integer':
                case 'smallint':
                case 'bigint':
                case 'decimal':
                case 'numeric':
                case 'real':
                case 'double precision':
                    type = 'number';
                    break;
                case 'date':
                    type = 'date';
                    break;
                case 'timestamp':
                case 'timestamptz':
                    type = 'datetime';
                    break;
                default:
                    type = 'string';
                    break;
            }

            return {
                name: row.column_name as string,
                type,
                key: !!row.is_primary_key
            };
        });

        return {
            name: tableName,
            connector: this.name,
            connection: '',
            connectionPayload,
            connectionPath,
            connectionTimeZone: '',
            columns
        };
    }

    /**
     * Create a table.
     * @param table Table to create.
     * @param connectionPayload Connection payload for auth.
     * @returns Created table.
     */
    async createTable(
        table: TableDefinition,
        connectionPayload: Record<string, string>
    ): Promise<TableDefinition> {
        await this.executeQuery(
            connectionPayload,
            format(
                `CREATE TABLE %I %s;`,
                table.connectionPath[0],
                table.columns
                    .map(col =>
                        format(
                            `(%I %s)`,
                            col.name,
                            this.mapToPostgresType(col.type) + (col.key ? ' PRIMARY KEY' : '')
                        )
                    )
                    .join(',')
            )
        );

        return table;
    }

    /**
     * Update a table.
     * @param oldTable Table to update.
     * @param newTable Updated Table.
     * @param connectionPayload Connection payload for auth.
     * @returns Updated table.
     */
    async updateTable(
        oldTable: TableDefinition,
        newTable: TableDefinition,
        connectionPayload: Record<string, string>
    ): Promise<TableDefinition> {
        const alterations: string[] = [];
        const oldColumnsMap = new Map(oldTable.columns.map(col => [col.name, col]));
        const newColumnsMap = new Map(newTable.columns.map(col => [col.name, col]));

        for (const oldCol of oldTable.columns) {
            const newCol = newColumnsMap.get(oldCol.name);
            if (!newCol) {
                alterations.push(format('DROP COLUMN %I', oldCol.name));
            } else if (oldCol.type !== newCol.type) {
                alterations.push(
                    format(
                        'ALTER COLUMN %I TYPE %s',
                        oldCol.name,
                        this.mapToPostgresType(newCol.type)
                    )
                );
            }
        }

        for (const newCol of newTable.columns) {
            if (!oldColumnsMap.has(newCol.name)) {
                alterations.push(
                    format(`ADD COLUMN %I %s`, newCol.name, this.mapToPostgresType(newCol.type))
                );
            }
        }

        const droppedColumns = oldTable.columns.filter(col => !newColumnsMap.has(col.name));
        const addedColumns = newTable.columns.filter(col => !oldColumnsMap.has(col.name));

        for (const droppedCol of droppedColumns) {
            const renamedMatchIndex = addedColumns.findIndex(
                addedCol => addedCol.type === droppedCol.type
            );

            if (renamedMatchIndex !== -1) {
                alterations.push(
                    format(
                        'RENAME COLUMN %I TO %I',
                        droppedCol.name,
                        addedColumns[renamedMatchIndex].name
                    )
                );

                droppedColumns.splice(droppedColumns.indexOf(droppedCol), 1);
                addedColumns.splice(renamedMatchIndex, 1);
            }
        }

        if (alterations.length > 0) {
            await this.executeQuery(
                connectionPayload as Record<string, string>,
                format(`ALTER TABLE %I %s;`, oldTable.connectionPath[0], alterations.join(',\n'))
            );
        }

        if (oldTable.connectionPath[0] !== newTable.connectionPath[0]) {
            await this.executeQuery(
                connectionPayload as Record<string, string>,
                format(
                    'ALTER TABLE %I RENAME TO %I;',
                    oldTable.connectionPath[0],
                    newTable.connectionPath[0]
                )
            );
        }

        return newTable;
    }

    /**
     * Delete a table.
     * @param table Table to delete.
     * @param connectionPayload Connection payload for auth.
     * @returns Deleted table.
     */
    async deleteTable(
        table: TableDefinition,
        connectionPayload: Record<string, string>
    ): Promise<TableDefinition> {
        await this.executeQuery(
            connectionPayload,
            format('DROP TABLE %I', table.connectionPath[0])
        );

        return table;
    }

    /**
     * Read data from tables.
     * @param tables Tables to read.
     * @param connectionPayload Connection payload for auth.
     * @returns Rows detail.
     */
    async readData(
        tables: TableDefinition[],
        connectionPayload: Record<string, string>
    ): Promise<{ table: TableDefinition; rows: Record<string, unknown>[] }[]> {
        return await Promise.all(
            tables.map(async table => ({
                table,
                rows: await this.executeQuery(
                    connectionPayload,
                    format('SELECT * FROM %I', table.connectionPath[0])
                )
            }))
        );
    }

    /**
     * Append data to the table.
     * @param table Table to append data into.
     * @param rows Rows to append.
     * @param connectionPayload Connection payload for auth.
     * @returns Resulting rows.
     */
    async createData(
        table: TableDefinition,
        rows: Record<string, unknown>[],
        connectionPayload: Record<string, string>
    ): Promise<Record<string, unknown>[]> {
        if (rows.length === 0) {
            return [];
        }

        const columns = table.columns.map(c => c.name);
        await this.executeQuery(
            connectionPayload,
            format(
                `INSERT INTO %I (%s) VALUES %s;`,
                table.connectionPath[0],
                format('%I', columns),
                rows
                    .map((_, i) => `(${columns.map((_, j) => `$${i * columns.length + j + 1}`)})`)
                    .join(',')
            ),
            rows.map(r => columns.map(c => r[c])).flat()
        );

        return rows;
    }

    /**
     * Update data in the table.
     * @param table Table to update data into.
     * @param rows Rows to update.
     * @param connectionPayload Connection payload for auth.
     * @returns Resulting rows.
     */
    async updateData(
        table: TableDefinition,
        rows: Record<string, unknown>[],
        connectionPayload: Record<string, string>
    ): Promise<Record<string, unknown>[]> {
        if (rows.length === 0) {
            return [];
        }

        const keyColumn = table.columns.find(col => col.key)?.name;
        if (!keyColumn) {
            throw new Error('Table definition must include a key column for updates.');
        }

        await this.executeQuery(
            connectionPayload,
            format(
                'UPDATE %I SET %s FROM (VALUES %s) AS updates (%s) WHERE %I.%I = updates.%I',
                table.connectionPath[0],
                table.columns
                    .filter(col => !col.key)
                    .map(col => format('%I = updates.%I', col.name, col.name))
                    .join(','),
                rows
                    .map(
                        (_, i) =>
                            `(${table.columns.map((_, idx) => `$${i * table.columns.length + idx + 1}`)})`
                    )
                    .join(','),
                table.columns.map(col => format('%I', col.name)).join(','),
                table.connectionPath[0],
                keyColumn,
                keyColumn
            ),
            rows.map(row => table.columns.map(col => row[col.name])).flat()
        );

        return rows;
    }

    /**
     * Delete data from the table.
     * @param table Table to delete data from.
     * @param rows Rows to delete.
     * @param connectionPayload Connection payload for auth.
     * @returns Deleted rows.
     */
    async deleteData(
        table: TableDefinition,
        rows: Record<string, unknown>[],
        connectionPayload: Record<string, string>
    ): Promise<Record<string, unknown>[]> {
        if (rows.length === 0) {
            return [];
        }

        const keyColumn = table.columns.find(col => col.key)?.name;
        if (!keyColumn) {
            throw new Error('Table definition must include a key column for deletion.');
        }

        await this.executeQuery(
            connectionPayload,
            format(
                'DELETE FROM %I WHERE %I IN (%s)',
                table.connectionPath[0],
                keyColumn,
                rows.map((_, idx) => `$${idx + 1}`).join(',')
            ),
            rows.map(row => row[keyColumn])
        );

        return rows;
    }
}
