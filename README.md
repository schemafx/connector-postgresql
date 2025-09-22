# SchemaFX

Build With [PostgreSQL](https://www.postgresql.org/).

### 📦 Installing

```Bash
npm install schemafx/connector-postgresql
```

### ⚙️ Customizing

```TS
import SchemaFX from 'schemafx';
import PostgreSQLConnector from 'schemafx-connector-postgresql';

new SchemaFX({
    // ...
    connectors: [
        // ...
        new PostgreSQLConnector({ name: 'postgresql' })
    ]
})
```

## 🤝 Contributing

- See [`CONTRIBUTING.md`](.github/CONTRIBUTING.md) for guidelines
- Issues, discussions, and PRs are welcome

## 📜 License

Apache 2.0 — see [LICENSE](LICENSE).

SchemaFX is community-driven. Contributions and new connectors are encouraged.
