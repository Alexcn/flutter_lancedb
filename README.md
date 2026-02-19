# flutter_lancedb

Flutter bindings for [LanceDB](https://lancedb.com/) - a developer-friendly, serverless vector database for AI applications.

## Features

- 🚀 **Fast vector search** - Perform similarity searches on vector embeddings
- 📦 **Local-first** - No server required, runs entirely on device
- 🔍 **SQL-like queries** - Filter and query your data with familiar syntax
- 📊 **Schema support** - Typed data with automatic schema inference
- 🔄 **CRUD operations** - Full create, read, update, delete support

## Getting Started

### Installation

Add this to your `pubspec.yaml`:

```yaml
dependencies:
  flutter_lancedb: ^0.1.0
```

### Initialization

Initialize the library before using it:

```dart
import 'package:flutter_lancedb/flutter_lancedb.dart';

Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();
  await RustLib.init();
  runApp(MyApp());
}
```

## Usage

### Connect to Database

```dart
// Connect to a local database
final db = await connect(uri: '/path/to/database');
```

### Create a Table with Data

```dart
import 'dart:convert';

final data = [
  {
    'id': 1,
    'text': 'Hello world',
    'vector': [0.1, 0.2, 0.3, 0.4],
  },
  {
    'id': 2, 
    'text': 'Goodbye world',
    'vector': [0.5, 0.6, 0.7, 0.8],
  },
];

final table = await db.createTable(
  name: 'my_vectors',
  dataJson: jsonEncode(data),
);
```

### Query Data

```dart
// Basic query with filter
final result = await table.query(
  columns: ['id', 'text'],
  filter: 'id > 0',
  limit: 10,
);

print('Found ${result.numRows} rows');
for (final row in result.rows) {
  print('id: ${row['id']}, text: ${row['text']}');
}
```

### Vector Search

```dart
// Find similar vectors
final queryVector = [0.3, 0.4, 0.5, 0.6];
final searchResult = await table
    .vectorSearch(vector: queryVector)
    .then((vq) => vq
        .distanceType(distanceType: DistanceType.cosine)
        .limit(limit: 5)
        .execute());

for (final row in searchResult.rows) {
  print('text: ${row['text']}, distance: ${row['_distance']}');
}
```

### Add Data

```dart
final newData = [
  {
    'id': 3,
    'text': 'New entry',
    'vector': [0.9, 1.0, 1.1, 1.2],
  },
];

await table.add(dataJson: jsonEncode(newData));
```

### Delete Data

```dart
await table.delete(predicate: 'id = 3');
```

### Update Data

```dart
await table.update(
  updatesJson: '{"text": "Updated text"}',
  predicate: 'id = 1',
);
```

## API Reference

### Connection Functions

| Function | Description |
|----------|-------------|
| `connect(uri)` | Connect to a LanceDB database |

### LanceConnection Methods

| Method | Description |
|--------|-------------|
| `tableNames()` | List all table names |
| `openTable(name)` | Open an existing table |
| `createTable(name, dataJson)` | Create a table with initial data |
| `createEmptyTable(name, schemaJson)` | Create an empty table with schema |
| `dropTable(name)` | Delete a table |

### LanceTable Methods

| Method | Description |
|--------|-------------|
| `name()` | Get table name |
| `schema()` | Get table schema |
| `countRows(filter?)` | Count rows (optionally filtered) |
| `query(columns?, filter?, limit?)` | Query data |
| `vectorSearch(vector, column?)` | Start a vector search |
| `add(dataJson)` | Add rows |
| `delete(predicate)` | Delete rows matching predicate |
| `update(updatesJson, predicate?)` | Update rows |
| `createIndex(column, indexType?, replace?)` | Create an index |
| `listIndices()` | List all indexes |
| `optimize()` | Optimize table storage |
| `version()` | Get table version |

### LanceVectorQuery Methods

| Method | Description |
|--------|-------------|
| `column(name)` | Set vector column |
| `distanceType(type)` | Set distance metric (L2, Cosine, Dot) |
| `limit(n)` | Limit results |
| `offset(n)` | Skip results |
| `onlyIf(predicate)` | Add filter |
| `postfilter()` | Enable post-filtering |
| `select(columns)` | Select columns |
| `nprobes(n)` | Set IVF probes |
| `refineFactor(n)` | Set refinement factor |
| `execute()` | Execute search |

## Supported Data Types

- `int32`, `int64` - Integer values
- `float32`, `float64` - Floating point values
- `string`, `utf8` - Text
- `bool`, `boolean` - Boolean values
- `fixed_size_list` - Vector embeddings

## Platform Support

| Platform | Status |
|----------|--------|
| macOS | ✅ Supported |
| iOS | ✅ Supported |
| Android | ✅ Supported |
| Linux | ✅ Supported |
| Windows | ✅ Supported |

## Example

See the [example](example/) directory for a complete Flutter app demonstrating all features.

## License

Apache 2.0 - see [LICENSE](LICENSE) for details.

## Credits

This package uses [flutter_rust_bridge](https://pub.dev/packages/flutter_rust_bridge) to provide Dart bindings for the [LanceDB Rust library](https://github.com/lancedb/lancedb).

